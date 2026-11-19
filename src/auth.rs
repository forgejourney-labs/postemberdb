//! Password authentication and role-based permissions for PostemberDB.
use crate::{protocol::crc32, DbError, KeyValueStore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use rand_core::OsRng;

const USER_PREFIX: &[u8] = b"AUTH/USER/";
const RECORD_MAGIC: &[u8; 4] = b"PAU1";
const MAX_USERNAME_BYTES: usize = 64;
const MAX_PASSWORD_BYTES: usize = 1024;
const KNOWN_PERMISSION_BITS: u32 = (1 << 6) - 1 | (1 << 31);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Permission {
    SqlRead = 0,
    SqlWrite = 1,
    KvRead = 2,
    KvWrite = 3,
    Ef7Read = 4,
    Ef7Write = 5,
    Admin = 31,
}

impl Permission {
    const fn mask(self) -> u32 { 1u32 << self as u8 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Permissions(u32);

impl Permissions {
    pub const NONE: Self = Self(0);
    pub const ADMIN: Self = Self(Permission::Admin.mask());
    pub const READ_ONLY: Self = Self(
        Permission::SqlRead.mask() | Permission::KvRead.mask() | Permission::Ef7Read.mask()
    );
    pub const READ_WRITE: Self = Self(
        Permission::SqlRead.mask() | Permission::SqlWrite.mask()
        | Permission::KvRead.mask() | Permission::KvWrite.mask()
        | Permission::Ef7Read.mask() | Permission::Ef7Write.mask()
    );

    pub fn from_bits(bits: u32) -> Result<Self, DbError> {
        if bits & !KNOWN_PERMISSION_BITS != 0 {
            return Err(DbError::Constraint("unknown RBAC permission bits".into()));
        }
        Ok(Self(bits))
    }

    pub const fn bits(self) -> u32 { self.0 }

    pub const fn allows(self, permission: Permission) -> bool {
        self.0 & Permission::Admin.mask() != 0 || self.0 & permission.mask() != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub username: String,
    pub permissions: Permissions,
}

impl Principal {
    pub fn allows(&self, permission: Permission) -> bool {
        self.permissions.allows(permission)
    }

    pub fn require(&self, permission: Permission) -> Result<(), DbError> {
        if self.allows(permission) {
            Ok(())
        } else {
            Err(DbError::Constraint(format!("user '{}' is not authorized for {:?}", self.username, permission)))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserRecord {
    password_hash: String,
    permissions: Permissions,
    enabled: bool,
}

impl UserRecord {
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        let hash = self.password_hash.as_bytes();
        if hash.is_empty() || hash.len() > u16::MAX as usize {
            return Err(DbError::Storage("invalid password hash length".into()));
        }
        let mut out = Vec::with_capacity(12 + hash.len());
        out.extend_from_slice(RECORD_MAGIC);
        out.push(1);
        out.push(u8::from(self.enabled));
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&self.permissions.bits().to_le_bytes());
        out.extend_from_slice(&(hash.len() as u16).to_le_bytes());
        out.extend_from_slice(hash);
        out.extend_from_slice(&crc32(&out).to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 18 || &bytes[..4] != RECORD_MAGIC || bytes[4] != 1
            || bytes[5] > 1 || bytes[6..8] != [0, 0]
            || u32::from_le_bytes(bytes[bytes.len()-4..].try_into().unwrap()) != crc32(&bytes[..bytes.len()-4]) {
            return Err(DbError::Storage("corrupt authentication user record".into()));
        }
        let permissions = Permissions::from_bits(u32::from_le_bytes(bytes[8..12].try_into().unwrap()))?;
        let hash_len = u16::from_le_bytes(bytes[12..14].try_into().unwrap()) as usize;
        if hash_len == 0 || 14usize.checked_add(hash_len).and_then(|n| n.checked_add(4)) != Some(bytes.len()) {
            return Err(DbError::Storage("invalid authentication hash length".into()));
        }
        let password_hash = String::from_utf8(bytes[14..14+hash_len].to_vec())
            .map_err(|_| DbError::Storage("authentication hash is not valid UTF-8".into()))?;
        PasswordHash::new(&password_hash)
            .map_err(|_| DbError::Storage("authentication hash has invalid PHC syntax".into()))?;
        Ok(Self { password_hash, permissions, enabled: bytes[5] == 1 })
    }
}

/// Persistent user directory. Passwords are never stored: only Argon2id PHC
/// hashes with per-user random salts are persisted in the auth sidecar.
pub struct AuthStore {
    users: KeyValueStore,
    dummy_hash: String,
}

impl AuthStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DbError> {
        let salt = SaltString::generate(&mut OsRng);
        let dummy_hash = Argon2::default()
            .hash_password(b"postemberdb-dummy-credential", &salt)
            .map_err(|_| DbError::Storage("could not initialize password verifier".into()))?
            .to_string();
        Ok(Self { users: KeyValueStore::open(path)?, dummy_hash })
    }

    pub fn create_user(
        &mut self,
        username: &str,
        password: &str,
        permissions: Permissions,
    ) -> Result<(), DbError> {
        let key = user_key(username)?;
        validate_password(password)?;
        if self.users.get(&key)?.is_some() {
            return Err(DbError::Constraint("user already exists".into()));
        }
        let salt = SaltString::generate(&mut OsRng);
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|_| DbError::Storage("password hashing failed".into()))?
            .to_string();
        let record = UserRecord { password_hash, permissions, enabled: true };
        self.users.put(&key, &record.encode()?)
    }

    /// Returns None for an unknown, disabled, or incorrectly authenticated user.
    /// Unknown usernames still perform an Argon2 verification against a dummy hash.
    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<Option<Principal>, DbError> {
        if password.len() > MAX_PASSWORD_BYTES {
            return Ok(None);
        }
        let key = user_key(username)?;
        let Some(bytes) = self.users.get(&key)? else {
            let parsed = PasswordHash::new(&self.dummy_hash)
                .map_err(|_| DbError::Storage("dummy password hash is invalid".into()))?;
            let _ = Argon2::default().verify_password(password.as_bytes(), &parsed);
            return Ok(None);
        };
        let record = UserRecord::decode(&bytes)?;
        let parsed = PasswordHash::new(&record.password_hash)
            .map_err(|_| DbError::Storage("stored password hash is invalid".into()))?;
        if Argon2::default().verify_password(password.as_bytes(), &parsed).is_err() || !record.enabled {
            return Ok(None);
        }
        Ok(Some(Principal { username: username.to_ascii_lowercase(), permissions: record.permissions }))
    }

    pub fn set_permissions(&mut self, username: &str, permissions: Permissions) -> Result<(), DbError> {
        let key = user_key(username)?;
        let bytes = self.users.get(&key)?
            .ok_or_else(|| DbError::Constraint("user does not exist".into()))?;
        let mut record = UserRecord::decode(&bytes)?;
        record.permissions = permissions;
        self.users.put(&key, &record.encode()?)
    }

    pub fn set_enabled(&mut self, username: &str, enabled: bool) -> Result<(), DbError> {
        let key = user_key(username)?;
        let bytes = self.users.get(&key)?
            .ok_or_else(|| DbError::Constraint("user does not exist".into()))?;
        let mut record = UserRecord::decode(&bytes)?;
        record.enabled = enabled;
        self.users.put(&key, &record.encode()?)
    }

    pub fn delete_user(&mut self, username: &str) -> Result<bool, DbError> {
        self.users.delete(&user_key(username)?)
    }

    pub fn list_users(&mut self) -> Result<Vec<String>, DbError> {
        let entries = self.users.scan_prefix(USER_PREFIX, None)?;
        entries.into_iter().map(|(key, _)| {
            String::from_utf8(key[USER_PREFIX.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication username key is invalid UTF-8".into()))
        }).collect()
    }
}

fn user_key(username: &str) -> Result<Vec<u8>, DbError> {
    if username.is_empty() || username.len() > MAX_USERNAME_BYTES
        || !username.is_ascii()
        || !username.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-@".contains(&byte)) {
        return Err(DbError::Constraint("username must be 1..=64 ASCII letters, digits, '.', '_' , '-' or '@'".into()));
    }
    let mut key = USER_PREFIX.to_vec();
    key.extend_from_slice(username.to_ascii_lowercase().as_bytes());
    Ok(key)
}

fn validate_password(password: &str) -> Result<(), DbError> {
    if password.len() < 12 || password.len() > MAX_PASSWORD_BYTES {
        return Err(DbError::Constraint("password must contain 12..=1024 bytes".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("postemberdb-auth-{}.auth",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[test]
    fn argon2_users_authenticate_and_permissions_are_enforced() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            auth.create_user("Reader", "correct horse battery staple", Permissions::READ_ONLY).unwrap();
            assert!(auth.authenticate("reader", "correct horse battery staple").unwrap().is_some());
            assert!(auth.authenticate("reader", "incorrect password").unwrap().is_none());
            let principal = auth.authenticate("READER", "correct horse battery staple").unwrap().unwrap();
            assert!(principal.allows(Permission::SqlRead));
            assert!(!principal.allows(Permission::SqlWrite));
            assert!(principal.require(Permission::SqlWrite).is_err());
            auth.set_permissions("reader", Permissions::ADMIN).unwrap();
            assert!(auth.authenticate("reader", "correct horse battery staple").unwrap().unwrap().allows(Permission::Ef7Write));
            auth.set_enabled("reader", false).unwrap();
            assert!(auth.authenticate("reader", "correct horse battery staple").unwrap().is_none());
        }
        for suffix in [".lock", ".pages", ".pages.wal"] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }
}
