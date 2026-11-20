//! Password authentication and role-based permissions for PostemberDB.
use crate::{protocol::crc32, DbError, KeyValueStore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use rand_core::OsRng;
use std::time::{SystemTime, UNIX_EPOCH};

const USER_PREFIX: &[u8] = b"AUTH/USER/";
const ROLE_PREFIX: &[u8] = b"AUTH/ROLE/";
const MEMBERSHIP_PREFIX: &[u8] = b"AUTH/MEMBER/";
const AUDIT_COUNTER_KEY: &[u8] = b"AUTH/AUDIT/NEXT";
const AUDIT_EVENT_PREFIX: &[u8] = b"AUTH/AUDIT/EVENT/";
const RECORD_MAGIC: &[u8; 4] = b"PAU1";
const AUDIT_MAGIC: &[u8; 4] = b"PAE1";
const ROLE_MAGIC: &[u8; 4] = b"PAR1";
const MAX_AUDIT_FIELD_BYTES: usize = 256;
const MAX_AUDIT_PAGE_SIZE: usize = 10_000;
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

    const fn union(self, other: Self) -> Self { Self(self.0 | other.0) }

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

/// Durable security event. Passwords, hashes, and tokens are never included.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub actor: String,
    pub action: String,
    pub subject: String,
    pub succeeded: bool,
}

impl AuditEvent {
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        let actor = self.actor.as_bytes();
        let action = self.action.as_bytes();
        let subject = self.subject.as_bytes();
        if actor.is_empty() || action.is_empty()
            || actor.len() > MAX_AUDIT_FIELD_BYTES
            || action.len() > MAX_AUDIT_FIELD_BYTES
            || subject.len() > MAX_AUDIT_FIELD_BYTES {
            return Err(DbError::Storage("invalid authentication audit field length".into()));
        }
        let mut out = Vec::with_capacity(32 + actor.len() + action.len() + subject.len());
        out.extend_from_slice(AUDIT_MAGIC);
        out.push(1);
        out.push(u8::from(self.succeeded));
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&self.timestamp_unix_ms.to_le_bytes());
        for field in [actor, action, subject] {
            out.extend_from_slice(&(field.len() as u16).to_le_bytes());
        }
        out.extend_from_slice(actor);
        out.extend_from_slice(action);
        out.extend_from_slice(subject);
        let checksum = crc32(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 32 || &bytes[..4] != AUDIT_MAGIC || bytes[4] != 1 || bytes[5] > 1
            || u32::from_le_bytes(bytes[bytes.len()-4..].try_into().unwrap()) != crc32(&bytes[..bytes.len()-4]) {
            return Err(DbError::Storage("corrupt authentication audit event".into()));
        }
        let sequence = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
        let timestamp_unix_ms = u64::from_le_bytes(bytes[14..22].try_into().unwrap());
        let actor_len = u16::from_le_bytes(bytes[22..24].try_into().unwrap()) as usize;
        let action_len = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
        let subject_len = u16::from_le_bytes(bytes[26..28].try_into().unwrap()) as usize;
        if sequence == 0 || actor_len == 0 || action_len == 0
            || [actor_len, action_len, subject_len].iter().any(|n| *n > MAX_AUDIT_FIELD_BYTES)
            || 28usize.checked_add(actor_len).and_then(|n| n.checked_add(action_len))
                .and_then(|n| n.checked_add(subject_len)).and_then(|n| n.checked_add(4)) != Some(bytes.len()) {
            return Err(DbError::Storage("invalid authentication audit event lengths".into()));
        }
        let mut offset = 28;
        let mut read_field = |length: usize| -> Result<String, DbError> {
            let end = offset + length;
            let value = String::from_utf8(bytes[offset..end].to_vec())
                .map_err(|_| DbError::Storage("authentication audit field is not UTF-8".into()))?;
            offset = end;
            Ok(value)
        };
        let actor = read_field(actor_len)?;
        let action = read_field(action_len)?;
        let subject = read_field(subject_len)?;
        Ok(Self { sequence, timestamp_unix_ms, actor, action, subject, succeeded: bytes[5] == 1 })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoleRecord {
    permissions: Permissions,
}

impl RoleRecord {
    fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(13);
        out.extend_from_slice(ROLE_MAGIC);
        out.push(1);
        out.extend_from_slice(&self.permissions.bits().to_le_bytes());
        let checksum = crc32(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != 13 || &bytes[..4] != ROLE_MAGIC || bytes[4] != 1
            || u32::from_le_bytes(bytes[9..13].try_into().unwrap()) != crc32(&bytes[..9]) {
            return Err(DbError::Storage("corrupt authentication role record".into()));
        }
        Ok(Self { permissions: Permissions::from_bits(u32::from_le_bytes(bytes[5..9].try_into().unwrap()))? })
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

/// Durable authentication identity metadata. Password hashes are never exposed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UserInfo {
    pub username: String,
    pub permissions: u32,
    pub enabled: bool,
    pub roles: Vec<String>,
}

/// Public role metadata. This structure never includes credential material.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoleInfo {
    pub name: String,
    pub permissions: u32,
}

/// Persistent user directory. Passwords are never stored: only Argon2id PHC
/// hashes with per-user random salts are persisted in the auth sidecar.
pub struct AuthStore {
    users: KeyValueStore,
    dummy_hash: String,
    audit_actor: Option<String>,
}

impl AuthStore {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, DbError> {
        let salt = SaltString::generate(&mut OsRng);
        let dummy_hash = Argon2::default()
            .hash_password(b"postemberdb-dummy-credential", &salt)
            .map_err(|_| DbError::Storage("could not initialize password verifier".into()))?
            .to_string();
        Ok(Self { users: KeyValueStore::open(path)?, dummy_hash, audit_actor: None })
    }

    fn commit_audited_mutation(
        &mut self,
        removals: &[Vec<u8>],
        upserts: &[(Vec<u8>, Vec<u8>)],
        actor: &str,
        action: &str,
        subject: &str,
        succeeded: bool,
    ) -> Result<(), DbError> {
        let current = match self.users.get(AUDIT_COUNTER_KEY)? {
            Some(bytes) if bytes.len() == 8 => u64::from_le_bytes(bytes.try_into().unwrap()),
            Some(_) => return Err(DbError::Storage("authentication audit counter is corrupt".into())),
            None => 0,
        };
        let sequence = current.checked_add(1)
            .ok_or_else(|| DbError::Storage("authentication audit sequence exhausted".into()))?;
        let timestamp_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)
            .unwrap_or_default().as_millis().min(u128::from(u64::MAX)) as u64;
        let effective_actor = if actor == "local-api" {
            self.audit_actor.as_deref().unwrap_or(actor).to_string()
        } else {
            actor.to_string()
        };
        let event = AuditEvent {
            sequence, timestamp_unix_ms,
            actor: effective_actor, action: action.to_string(),
            subject: subject.to_string(), succeeded,
        };
        let mut batch = upserts.to_vec();
        batch.push((AUDIT_COUNTER_KEY.to_vec(), sequence.to_le_bytes().to_vec()));
        batch.push((audit_event_key(sequence), event.encode()?));
        self.users.apply_batch(removals, &batch)?;
        Ok(())
    }

    /// Runs trusted administrative AuthStore operations as an authenticated principal.
    /// The principal must hold Admin; every audit event emitted by the closure records
    /// that principal instead of the legacy local-api actor. The actor context is
    /// cleared before this method returns, including when the operation returns an error.
    pub fn as_admin<T>(
        &mut self,
        principal: &Principal,
        operation: impl FnOnce(&mut Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        principal.require(Permission::Admin)?;
        if self.audit_actor.is_some() {
            return Err(DbError::Constraint("nested administrative audit contexts are not allowed".into()));
        }
        self.audit_actor = Some(principal.username.clone());
        let result = operation(self);
        self.audit_actor = None;
        result
    }

    /// Reads a bounded sequence of durable audit events after a sequence number.
    /// Sequence-addressed reads avoid scanning the complete audit history.
    pub fn audit_events(&mut self, after_sequence: u64, limit: usize) -> Result<Vec<AuditEvent>, DbError> {
        if limit > MAX_AUDIT_PAGE_SIZE {
            return Err(DbError::Constraint(format!("audit page cannot exceed {MAX_AUDIT_PAGE_SIZE} events")));
        }
        if limit == 0 { return Ok(Vec::new()); }
        let current = match self.users.get(AUDIT_COUNTER_KEY)? {
            Some(bytes) if bytes.len() == 8 => u64::from_le_bytes(bytes.try_into().unwrap()),
            Some(_) => return Err(DbError::Storage("authentication audit counter is corrupt".into())),
            None => 0,
        };
        let mut events = Vec::new();
        let mut sequence = after_sequence.saturating_add(1);
        while sequence <= current && events.len() < limit {
            let bytes = self.users.get(&audit_event_key(sequence))?
                .ok_or_else(|| DbError::Storage(format!("authentication audit event {sequence} is missing")))?;
            let event = AuditEvent::decode(&bytes)?;
            if event.sequence != sequence {
                return Err(DbError::Storage("authentication audit sequence/key mismatch".into()));
            }
            events.push(event);
            sequence = sequence.saturating_add(1);
        }
        Ok(events)
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
        self.commit_audited_mutation(&[], &[(key, record.encode()?)], "local-api", "user.create", username, true)
    }

    /// Returns None for an unknown, disabled, or incorrectly authenticated user.
    /// Unknown usernames still perform an Argon2 verification against a dummy hash.
    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<Option<Principal>, DbError> {
        let key = user_key(username)?;
        if password.len() > MAX_PASSWORD_BYTES {
            self.commit_audited_mutation(&[], &[], username, "auth.login", username, false)?;
            return Ok(None);
        }
        let Some(bytes) = self.users.get(&key)? else {
            let parsed = PasswordHash::new(&self.dummy_hash)
                .map_err(|_| DbError::Storage("dummy password hash is invalid".into()))?;
            let _ = Argon2::default().verify_password(password.as_bytes(), &parsed);
            self.commit_audited_mutation(&[], &[], username, "auth.login", username, false)?;
            return Ok(None);
        };
        let record = UserRecord::decode(&bytes)?;
        let parsed = PasswordHash::new(&record.password_hash)
            .map_err(|_| DbError::Storage("stored password hash is invalid".into()))?;
        if Argon2::default().verify_password(password.as_bytes(), &parsed).is_err() || !record.enabled {
            self.commit_audited_mutation(&[], &[], username, "auth.login", username, false)?;
            return Ok(None);
        }
        let mut effective_permissions = record.permissions;
        for role in self.list_user_roles(username)? {
            let bytes = self.users.get(&role_key(&role)?)?
                .ok_or_else(|| DbError::Storage(format!("assigned authentication role '{role}' is missing")))?;
            effective_permissions = effective_permissions.union(RoleRecord::decode(&bytes)?.permissions);
        }
        self.commit_audited_mutation(&[], &[], username, "auth.login", username, true)?;
        Ok(Some(Principal { username: username.to_ascii_lowercase(), permissions: effective_permissions }))
    }

    /// Replace a user's password hash without exposing or returning password material.
    /// The new credential is independently salted and persisted before this method returns.
    pub fn change_password(&mut self, username: &str, new_password: &str) -> Result<(), DbError> {
        validate_password(new_password)?;
        let key = user_key(username)?;
        let bytes = self.users.get(&key)?
            .ok_or_else(|| DbError::Constraint("user does not exist".into()))?;
        let mut record = UserRecord::decode(&bytes)?;
        let salt = SaltString::generate(&mut OsRng);
        record.password_hash = Argon2::default()
            .hash_password(new_password.as_bytes(), &salt)
            .map_err(|_| DbError::Storage("password hashing failed".into()))?
            .to_string();
        self.commit_audited_mutation(&[], &[(key, record.encode()?)], "local-api", "user.password.change", username, true)
    }

    pub fn set_permissions(&mut self, username: &str, permissions: Permissions) -> Result<(), DbError> {
        let key = user_key(username)?;
        let bytes = self.users.get(&key)?
            .ok_or_else(|| DbError::Constraint("user does not exist".into()))?;
        let mut record = UserRecord::decode(&bytes)?;
        record.permissions = permissions;
        self.commit_audited_mutation(&[], &[(key, record.encode()?)], "local-api", "user.permissions.change", username, true)
    }

    pub fn set_enabled(&mut self, username: &str, enabled: bool) -> Result<(), DbError> {
        let key = user_key(username)?;
        let bytes = self.users.get(&key)?
            .ok_or_else(|| DbError::Constraint("user does not exist".into()))?;
        let mut record = UserRecord::decode(&bytes)?;
        record.enabled = enabled;
        self.commit_audited_mutation(&[], &[(key, record.encode()?)], "local-api", if enabled { "user.enable" } else { "user.disable" }, username, true)
    }

    pub fn delete_user(&mut self, username: &str) -> Result<bool, DbError> {
        let key = user_key(username)?;
        if self.users.get(&key)?.is_none() { return Ok(false); }
        let mut membership_prefix = MEMBERSHIP_PREFIX.to_vec();
        membership_prefix.extend_from_slice(&normalized_identifier(username)?);
        membership_prefix.push(b'/');
        let memberships = self.users.scan_prefix(&membership_prefix, None)?;
        if memberships.len() > 4_998 {
            return Err(DbError::Constraint("user has too many role memberships to delete atomically".into()));
        }
        let mut removals = vec![key];
        removals.extend(memberships.into_iter().map(|(key, _)| key));
        self.commit_audited_mutation(&removals, &[], "local-api", "user.delete", username, true)?;
        Ok(true)
    }

    /// Deletes a role and all its user memberships in one atomic page-tree batch.
    pub fn delete_role(&mut self, name: &str) -> Result<bool, DbError> {
        let key = role_key(name)?;
        if self.users.get(&key)?.is_none() { return Ok(false); }
        let role_suffix = [vec![b'/'], normalized_identifier(name)?].concat();
        let memberships = self.users.scan_prefix(MEMBERSHIP_PREFIX, None)?
            .into_iter()
            .filter(|(membership, _)| membership.ends_with(&role_suffix))
            .map(|(membership, _)| membership)
            .collect::<Vec<_>>();
        if memberships.len() > 4_999 {
            return Err(DbError::Constraint("role has too many members to delete atomically".into()));
        }
        let mut removals = vec![key];
        removals.extend(memberships);
        self.commit_audited_mutation(&removals, &[], "local-api", "role.delete", name, true)?;
        Ok(true)
    }

    /// Creates or replaces a named role with the supplied permission set.
    pub fn create_role(&mut self, name: &str, permissions: Permissions) -> Result<(), DbError> {
        let key = role_key(name)?;
        self.commit_audited_mutation(
            &[], &[(key, RoleRecord { permissions }.encode())],
            "local-api", "role.create", name, true,
        )
    }

    /// Replaces the permissions granted by a role. Existing memberships take
    /// effect immediately on the next authentication.
    pub fn set_role_permissions(&mut self, name: &str, permissions: Permissions) -> Result<(), DbError> {
        let key = role_key(name)?;
        if self.users.get(&key)?.is_none() {
            return Err(DbError::Constraint("role does not exist".into()));
        }
        self.commit_audited_mutation(
            &[], &[(key, RoleRecord { permissions }.encode())],
            "local-api", "role.permissions.change", name, true,
        )
    }

    /// Assigns an existing role to an existing user.
    pub fn assign_role(&mut self, username: &str, role: &str) -> Result<(), DbError> {
        let user = user_key(username)?;
        let role_key_value = role_key(role)?;
        if self.users.get(&user)?.is_none() {
            return Err(DbError::Constraint("user does not exist".into()));
        }
        if self.users.get(&role_key_value)?.is_none() {
            return Err(DbError::Constraint("role does not exist".into()));
        }
        let key = membership_key(username, role)?;
        if self.users.get(&key)?.is_some() {
            return Err(DbError::Constraint("role is already assigned to user".into()));
        }
        self.commit_audited_mutation(
            &[], &[(key, vec![1])], "local-api", "role.assign",
            &format!("{username}:{role}"), true,
        )
    }

    /// Removes an existing user/role membership.
    pub fn revoke_role(&mut self, username: &str, role: &str) -> Result<bool, DbError> {
        let key = membership_key(username, role)?;
        if self.users.get(&key)?.is_none() { return Ok(false); }
        self.commit_audited_mutation(
            &[key], &[], "local-api", "role.revoke",
            &format!("{username}:{role}"), true,
        )?;
        Ok(true)
    }

    pub fn list_roles(&mut self) -> Result<Vec<String>, DbError> {
        let entries = self.users.scan_prefix(ROLE_PREFIX, None)?;
        entries.into_iter().map(|(key, value)| {
            let name = String::from_utf8(key[ROLE_PREFIX.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication role name is invalid UTF-8".into()))?;
            RoleRecord::decode(&value)?;
            Ok(name)
        }).collect()
    }

    pub fn list_user_roles(&mut self, username: &str) -> Result<Vec<String>, DbError> {
        let mut prefix = MEMBERSHIP_PREFIX.to_vec();
        prefix.extend_from_slice(user_key(username)?[USER_PREFIX.len()..].as_ref());
        prefix.push(b'/');
        let entries = self.users.scan_prefix(&prefix, None)?;
        entries.into_iter().map(|(key, value)| {
            if value != [1] {
                return Err(DbError::Storage("invalid authentication role membership record".into()));
            }
            String::from_utf8(key[prefix.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication role membership name is invalid UTF-8".into()))
        }).collect()
    }

    pub fn list_users(&mut self) -> Result<Vec<String>, DbError> {
        let entries = self.users.scan_prefix(USER_PREFIX, None)?;
        entries.into_iter().map(|(key, _)| {
            String::from_utf8(key[USER_PREFIX.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication username key is invalid UTF-8".into()))
        }).collect()
    }

    /// Returns bounded public user metadata without revealing password hashes.
    pub fn list_user_details(&mut self) -> Result<Vec<UserInfo>, DbError> {
        let entries = self.users.scan_prefix(USER_PREFIX, None)?;
        let mut users = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let username = String::from_utf8(key[USER_PREFIX.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication username key is invalid UTF-8".into()))?;
            let record = UserRecord::decode(&value)?;
            let roles = self.list_user_roles(&username)?;
            users.push(UserInfo {
                username,
                permissions: record.permissions.bits(),
                enabled: record.enabled,
                roles,
            });
        }
        Ok(users)
    }

    /// Returns public named-role metadata without exposing internal records.
    pub fn list_role_details(&mut self) -> Result<Vec<RoleInfo>, DbError> {
        let entries = self.users.scan_prefix(ROLE_PREFIX, None)?;
        entries.into_iter().map(|(key, value)| {
            let name = String::from_utf8(key[ROLE_PREFIX.len()..].to_vec())
                .map_err(|_| DbError::Storage("authentication role name is invalid UTF-8".into()))?;
            let record = RoleRecord::decode(&value)?;
            Ok(RoleInfo { name, permissions: record.permissions.bits() })
        }).collect()
    }
}

fn role_key(name: &str) -> Result<Vec<u8>, DbError> {
    let identifier = normalized_identifier(name)?;
    let mut key = ROLE_PREFIX.to_vec();
    key.extend_from_slice(&identifier);
    Ok(key)
}

fn membership_key(username: &str, role: &str) -> Result<Vec<u8>, DbError> {
    let user = normalized_identifier(username)?;
    let role = normalized_identifier(role)?;
    let mut key = MEMBERSHIP_PREFIX.to_vec();
    key.extend_from_slice(&user);
    key.push(b'/');
    key.extend_from_slice(&role);
    Ok(key)
}

fn normalized_identifier(value: &str) -> Result<Vec<u8>, DbError> {
    if value.is_empty() || value.len() > MAX_USERNAME_BYTES || !value.is_ascii()
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-@".contains(&byte)) {
        return Err(DbError::Constraint("identifier must be 1..=64 ASCII letters, digits, '.', '_', '-' or '@'".into()));
    }
    Ok(value.to_ascii_lowercase().into_bytes())
}

fn audit_event_key(sequence: u64) -> Vec<u8> {
    let mut key = AUDIT_EVENT_PREFIX.to_vec();
    key.extend_from_slice(&sequence.to_be_bytes());
    key
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
    fn password_change_replaces_the_credential_and_survives_reopen() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            auth.create_user("rotation", "first secure password", Permissions::READ_WRITE).unwrap();
            auth.change_password("rotation", "second secure password").unwrap();
            assert!(auth.authenticate("rotation", "first secure password").unwrap().is_none());
            assert!(auth.authenticate("rotation", "second secure password").unwrap().is_some());
        }
        {
            let mut auth = AuthStore::open(&path).unwrap();
            assert!(auth.authenticate("rotation", "first secure password").unwrap().is_none());
            assert!(auth.authenticate("rotation", "second secure password").unwrap().is_some());
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal"] {
            let candidate = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
    }

    #[test]
    fn audit_events_are_durable_bounded_and_never_contain_password_material() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            auth.create_user("audit-user", "correct horse battery staple", Permissions::READ_ONLY).unwrap();
            assert!(auth.authenticate("audit-user", "incorrect password").unwrap().is_none());
            assert!(auth.authenticate("audit-user", "correct horse battery staple").unwrap().is_some());
            auth.set_permissions("audit-user", Permissions::READ_WRITE).unwrap();
            auth.set_enabled("audit-user", false).unwrap();
            assert!(auth.delete_user("audit-user").unwrap());
            let events = auth.audit_events(0, 100).unwrap();
            assert_eq!(events.len(), 6);
            assert_eq!(events[0].action, "user.create");
            assert_eq!(events[1].action, "auth.login");
            assert!(!events[1].succeeded);
            assert!(events[2].succeeded);
            assert_eq!(events[3].action, "user.permissions.change");
            assert_eq!(events[4].action, "user.disable");
            assert_eq!(events[5].action, "user.delete");
            assert!(auth.audit_events(3, 2).unwrap().iter().all(|event| event.sequence > 3));
            assert!(auth.audit_events(0, MAX_AUDIT_PAGE_SIZE + 1).is_err());
        }
        {
            let mut auth = AuthStore::open(&path).unwrap();
            let events = auth.audit_events(0, 100).unwrap();
            assert_eq!(events.len(), 6);
            let encoded = events.iter().flat_map(|event| {
                [event.actor.as_bytes(), event.action.as_bytes(), event.subject.as_bytes()]
            }).flatten().copied().collect::<Vec<_>>();
            assert!(!encoded.windows(b"correct horse battery staple".len())
                .any(|window| window == b"correct horse battery staple"));
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal"] {
            let candidate = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
    }

    #[test]
    fn deleting_users_and_roles_cleans_memberships_without_regranting_stale_access() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            auth.create_user("membership-user", "correct horse battery staple", Permissions::NONE).unwrap();
            auth.create_role("moderators", Permissions::ADMIN).unwrap();
            auth.assign_role("membership-user", "moderators").unwrap();
            assert!(auth.delete_user("membership-user").unwrap());
            auth.create_user("membership-user", "another secure password", Permissions::NONE).unwrap();
            let principal = auth.authenticate("membership-user", "another secure password").unwrap().unwrap();
            assert!(!principal.allows(Permission::Admin));
            auth.assign_role("membership-user", "moderators").unwrap();
            assert!(auth.delete_role("moderators").unwrap());
            let principal = auth.authenticate("membership-user", "another secure password").unwrap().unwrap();
            assert!(!principal.allows(Permission::Admin));
            assert!(auth.list_user_roles("membership-user").unwrap().is_empty());
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal"] {
            let candidate = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
    }

    #[test]
    fn admin_operations_require_admin_and_record_authenticated_actor() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            let reader = Principal { username: "reader".into(), permissions: Permissions::READ_ONLY };
            assert!(auth.as_admin(&reader, |store| {
                store.create_user("blocked", "correct horse battery staple", Permissions::NONE)
            }).is_err());
            let admin = Principal { username: "root-operator".into(), permissions: Permissions::ADMIN };
            auth.as_admin(&admin, |store| {
                store.create_user("managed-user", "correct horse battery staple", Permissions::READ_ONLY)?;
                store.set_permissions("managed-user", Permissions::READ_WRITE)?;
                Ok(())
            }).unwrap();
            let events = auth.audit_events(0, 20).unwrap();
            let admin_events = events.iter().filter(|event| event.subject == "managed-user").collect::<Vec<_>>();
            assert_eq!(admin_events.len(), 2);
            assert!(admin_events.iter().all(|event| event.actor == "root-operator"));
            assert!(auth.create_user("local-user", "another secure password", Permissions::NONE).is_ok());
            let events = auth.audit_events(0, 20).unwrap();
            assert_eq!(events.last().unwrap().actor, "local-api");
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal"] {
            let candidate = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
    }

    #[test]
    fn named_roles_grant_and_revoke_permissions_durably() {
        let path = path();
        {
            let mut auth = AuthStore::open(&path).unwrap();
            auth.create_user("role-user", "correct horse battery staple", Permissions::NONE).unwrap();
            auth.create_role("builders", Permissions::READ_WRITE).unwrap();
            auth.assign_role("role-user", "builders").unwrap();
            let principal = auth.authenticate("role-user", "correct horse battery staple").unwrap().unwrap();
            assert!(principal.allows(Permission::SqlWrite));
            assert_eq!(auth.list_user_roles("role-user").unwrap(), vec!["builders"]);
            auth.set_role_permissions("builders", Permissions::READ_ONLY).unwrap();
            let principal = auth.authenticate("role-user", "correct horse battery staple").unwrap().unwrap();
            assert!(principal.allows(Permission::SqlRead));
            assert!(!principal.allows(Permission::SqlWrite));
            assert!(auth.revoke_role("role-user", "builders").unwrap());
            let principal = auth.authenticate("role-user", "correct horse battery staple").unwrap().unwrap();
            assert!(!principal.allows(Permission::SqlRead));
        }
        {
            let mut auth = AuthStore::open(&path).unwrap();
            assert_eq!(auth.list_roles().unwrap(), vec!["builders"]);
            assert!(auth.list_user_roles("role-user").unwrap().is_empty());
            assert!(auth.audit_events(0, 100).unwrap().iter().any(|event| event.action == "role.assign"));
        }
        for suffix in ["", ".lock", ".pages", ".pages.wal"] {
            let candidate = std::path::PathBuf::from(format!("{}{}", path.display(), suffix));
            let _ = fs::remove_file(candidate);
        }
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
