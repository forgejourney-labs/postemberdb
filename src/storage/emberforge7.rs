//! EmberForge 7 Engine (EF7): a Minecraft/KSL-oriented persistent record layer.
//!
//! EF7 builds on the PDB6 page allocator and CoW B+ tree, but owns a separate
//! sidecar and a record format designed for large game records and UUID lookups.
//! Payload pages are synced before the descriptor/index root is published.

use super::{extent::{self, ExtentFile}, page_tree};
use crate::DbError;
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORD_PAGES: usize = 16_384;
const CHUNK_MAGIC: &[u8; 4] = b"EF7C";
const DESCRIPTOR_MAGIC: &[u8; 4] = b"EF7R";
const CHUNK_HEADER: usize = 41;
const CHUNK_CAPACITY: usize = extent::page_payload_size() - CHUNK_HEADER;
const KEY_NAMESPACE: u8 = 0x50;
const KEY_RECORD: u8 = 0x51;
const KEY_PLAYER_UUID: u8 = 0x52;
const KEY_PLAYER_NAME: u8 = 0x53;
const KEY_PLAYER_ROW_META: u8 = 0x54;
const KEY_CHUNK_DATA: u8 = 0x55;
const KEY_NAMESPACE_NAME: u8 = 0x56;
const FORMAT_VERSION: u8 = 1;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EmberForge7NamespaceKind {
    Server = 1, World = 2, Plugin = 3, PlayerData = 4, Custom = 5,
}
impl EmberForge7NamespaceKind {
    fn decode(value: u8) -> Result<Self, DbError> {
        match value { 1 => Ok(Self::Server), 2 => Ok(Self::World), 3 => Ok(Self::Plugin),
            4 => Ok(Self::PlayerData), 5 => Ok(Self::Custom),
            _ => Err(DbError::Storage("unknown EF7 namespace kind".into())) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmberForge7Namespace {
    pub id: u64,
    pub parent_id: Option<u64>,
    pub kind: EmberForge7NamespaceKind,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmberForge7PlayerProfile {
    pub uuid: [u8; 16],
    pub username: String,
    pub first_joined_ms: u64,
    pub last_seen_ms: u64,
    pub playtime_ticks: u64,
    pub custom_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlayerIdentity {
    uuid: [u8; 16],
    username: Option<String>,
}

fn normalize_namespace_name(name: &str) -> Result<String, DbError> {
    if name.is_empty() || name.len() > 128 || !name.is_ascii() {
        return Err(DbError::Constraint("EF7 namespace names must contain 1..=128 ASCII characters".into()));
    }
    let normalized = name.to_ascii_lowercase();
    if normalized == "." || normalized == ".."
        || !normalized.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-' | b'.'))
    {
        return Err(DbError::Constraint("EF7 namespace names may contain only letters, digits, '.', '_' and '-'".into()));
    }
    Ok(normalized)
}

fn normalize_player_name(name: &str) -> Result<String, DbError> {
    if name.is_empty() || name.len() > 16 || !name.is_ascii()
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(DbError::Constraint("Minecraft player names must contain 1..=16 ASCII letters, digits or '_'".into()));
    }
    Ok(name.to_ascii_lowercase())
}

fn namespace_key(id: u64) -> Result<Vec<u8>, DbError> {
    if id == 0 { return Err(DbError::Constraint("EF7 namespace ID must be non-zero".into())); }
    let mut key = vec![KEY_NAMESPACE];
    key.extend_from_slice(&id.to_be_bytes());
    Ok(key)
}

fn namespace_name_key(parent_id: Option<u64>, name: &str) -> Result<Vec<u8>, DbError> {
    if parent_id == Some(0) {
        return Err(DbError::Constraint("EF7 namespace parent ID must be non-zero".into()));
    }
    let normalized = normalize_namespace_name(name)?;
    let mut key = vec![KEY_NAMESPACE_NAME];
    key.extend_from_slice(&parent_id.unwrap_or(0).to_be_bytes());
    key.extend_from_slice(normalized.as_bytes());
    Ok(key)
}

fn player_name_key(namespace_id: u64, name: &str) -> Result<Vec<u8>, DbError> {
    let normalized = normalize_player_name(name)?;
    let mut key = vec![KEY_PLAYER_NAME];
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(normalized.as_bytes());
    Ok(key)
}

fn player_identity_key(namespace_id: u64, row_id: u64) -> Result<Vec<u8>, DbError> {
    if namespace_id == 0 || row_id == 0 {
        return Err(DbError::Constraint("EF7 namespace and player row IDs must be non-zero".into()));
    }
    let mut key = vec![KEY_PLAYER_ROW_META];
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(&row_id.to_be_bytes());
    Ok(key)
}


fn chunk_key(namespace_id: u64, chunk_x: i32, chunk_z: i32) -> Result<Vec<u8>, DbError> {
    if namespace_id == 0 {
        return Err(DbError::Constraint("EF7 namespace ID must be non-zero".into()));
    }
    let mut key = Vec::with_capacity(17);
    key.push(KEY_CHUNK_DATA);
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(&((chunk_x as u32) ^ 0x8000_0000).to_be_bytes());
    key.extend_from_slice(&((chunk_z as u32) ^ 0x8000_0000).to_be_bytes());
    Ok(key)
}

fn decode_chunk_coordinates(key: &[u8]) -> Result<(u64, i32, i32), DbError> {
    if key.len() != 17 || key[0] != KEY_CHUNK_DATA {
        return Err(DbError::Storage("invalid EF7 chunk index key".into()));
    }
    let namespace_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
    let x = u32::from_be_bytes(key[9..13].try_into().unwrap()) ^ 0x8000_0000;
    let z = u32::from_be_bytes(key[13..17].try_into().unwrap()) ^ 0x8000_0000;
    if namespace_id == 0 {
        return Err(DbError::Storage("EF7 chunk key contains reserved namespace zero".into()));
    }
    Ok((namespace_id, x as i32, z as i32))
}

fn prefix_upper_key(prefix: &[u8]) -> Vec<u8> {
    let mut upper = prefix.to_vec();
    upper.resize(1024, 0xff);
    upper
}

impl EmberForge7Namespace {
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        if self.id == 0 || self.parent_id == Some(0) || self.parent_id == Some(self.id) {
            return Err(DbError::Constraint("invalid EF7 namespace identity or parent".into()));
        }
        if normalize_namespace_name(&self.name)? != self.name {
            return Err(DbError::Constraint("EF7 namespace names must be canonical lowercase".into()));
        }
        let mut out = Vec::with_capacity(34 + self.name.len());
        out.extend_from_slice(b"EF7N");
        out.extend_from_slice(&[FORMAT_VERSION, self.kind as u8, u8::from(self.parent_id.is_some()), 0]);
        out.extend_from_slice(&self.id.to_le_bytes());
        out.extend_from_slice(&self.parent_id.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&(self.name.len() as u16).to_le_bytes());
        out.extend_from_slice(self.name.as_bytes());
        let sum = checksum(&out);
        out.extend_from_slice(&sum.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 34 || &bytes[..4] != b"EF7N" || bytes[4] != FORMAT_VERSION
            || bytes[6] > 1 || bytes[7] != 0 {
            return Err(DbError::Storage("invalid EF7 namespace record header".into()));
        }
        let name_len = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
        if bytes.len() != 34 + name_len {
            return Err(DbError::Storage("invalid EF7 namespace record length".into()));
        }
        let expected = u64::from_le_bytes(bytes[26 + name_len..34 + name_len].try_into().unwrap());
        if checksum(&bytes[..26 + name_len]) != expected {
            return Err(DbError::Storage("EF7 namespace record checksum mismatch".into()));
        }
        let id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let parent = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let name = std::str::from_utf8(&bytes[26..26 + name_len])
            .map_err(|_| DbError::Storage("EF7 namespace name is not UTF-8".into()))?.to_owned();
        if id == 0 || normalize_namespace_name(&name).map_err(|_| DbError::Storage("invalid EF7 namespace name".into()))? != name
            || (bytes[6] == 0 && parent != 0) || (bytes[6] == 1 && (parent == 0 || parent == id)) {
            return Err(DbError::Storage("invalid EF7 namespace fields".into()));
        }
        Ok(Self { id, parent_id: if bytes[6] == 1 { Some(parent) } else { None },
            kind: EmberForge7NamespaceKind::decode(bytes[5])?, name })
    }
}

impl PlayerIdentity {
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        let username = match &self.username { Some(n) => normalize_player_name(n)?, None => String::new() };
        let mut out = Vec::with_capacity(30 + username.len());
        out.extend_from_slice(b"EF7I");
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&self.uuid);
        out.push(username.len() as u8);
        out.extend_from_slice(username.as_bytes());
        let sum = checksum(&out);
        out.extend_from_slice(&sum.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 30 || &bytes[..4] != b"EF7I" || bytes[4] != FORMAT_VERSION {
            return Err(DbError::Storage("invalid EF7 player identity header".into()));
        }
        let name_len = bytes[21] as usize;
        if name_len > 16 || bytes.len() != 30 + name_len {
            return Err(DbError::Storage("invalid EF7 player identity length".into()));
        }
        let expected = u64::from_le_bytes(bytes[22 + name_len..30 + name_len].try_into().unwrap());
        if checksum(&bytes[..22 + name_len]) != expected {
            return Err(DbError::Storage("EF7 player identity checksum mismatch".into()));
        }
        let username = if name_len == 0 { None } else {
            let name = std::str::from_utf8(&bytes[22..22 + name_len])
                .map_err(|_| DbError::Storage("EF7 player name is not UTF-8".into()))?;
            let normalized = normalize_player_name(name)
                .map_err(|_| DbError::Storage("invalid EF7 player identity name".into()))?;
            if normalized != name { return Err(DbError::Storage("EF7 player identity name is not canonical".into())); }
            Some(normalized)
        };
        Ok(Self { uuid: bytes[5..21].try_into().unwrap(), username })
    }
}

impl EmberForge7PlayerProfile {
    fn encode(&self) -> Result<Vec<u8>, DbError> {
        normalize_player_name(&self.username)?;
        if self.last_seen_ms < self.first_joined_ms {
            return Err(DbError::Constraint("player last_seen_ms cannot be earlier than first_joined_ms".into()));
        }
        const HEADER: usize = 58;
        if self.custom_data.len().saturating_add(HEADER + self.username.len()) > MAX_RECORD_BYTES {
            return Err(DbError::Constraint("EF7 player profile exceeds the 64 MiB record limit".into()));
        }
        let mut out = Vec::with_capacity(HEADER + self.username.len() + self.custom_data.len());
        out.extend_from_slice(b"EF7P");
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&self.uuid);
        out.push(self.username.len() as u8);
        out.extend_from_slice(self.username.as_bytes());
        out.extend_from_slice(&self.first_joined_ms.to_le_bytes());
        out.extend_from_slice(&self.last_seen_ms.to_le_bytes());
        out.extend_from_slice(&self.playtime_ticks.to_le_bytes());
        out.extend_from_slice(&(self.custom_data.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.custom_data);
        let sum = checksum(&out);
        out.extend_from_slice(&sum.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 59 || &bytes[..4] != b"EF7P" || bytes[4] != FORMAT_VERSION {
            return Err(DbError::Storage("invalid EF7 player profile header".into()));
        }
        let name_len = bytes[21] as usize;
        if name_len == 0 || name_len > 16 || bytes.len() < 58 + name_len {
            return Err(DbError::Storage("invalid EF7 player profile username length".into()));
        }
        let fixed = 22 + name_len;
        let first_joined_ms = u64::from_le_bytes(bytes[fixed..fixed + 8].try_into().unwrap());
        let last_seen_ms = u64::from_le_bytes(bytes[fixed + 8..fixed + 16].try_into().unwrap());
        let playtime_ticks = u64::from_le_bytes(bytes[fixed + 16..fixed + 24].try_into().unwrap());
        let data_len = u32::from_le_bytes(bytes[fixed + 24..fixed + 28].try_into().unwrap()) as usize;
        if bytes.len() != fixed + 28 + data_len + 8 {
            return Err(DbError::Storage("EF7 player profile length mismatch".into()));
        }
        let expected = u64::from_le_bytes(bytes[fixed + 28 + data_len..].try_into().unwrap());
        if checksum(&bytes[..fixed + 28 + data_len]) != expected || last_seen_ms < first_joined_ms {
            return Err(DbError::Storage("EF7 player profile checksum or timestamp validation failed".into()));
        }
        let username = std::str::from_utf8(&bytes[22..fixed])
            .map_err(|_| DbError::Storage("EF7 player profile name is not UTF-8".into()))?.to_owned();
        normalize_player_name(&username).map_err(|_| DbError::Storage("invalid EF7 player profile name".into()))?;
        Ok(Self { uuid: bytes[5..21].try_into().unwrap(), username, first_joined_ms, last_seen_ms,
            playtime_ticks, custom_data: bytes[fixed + 28..fixed + 28 + data_len].to_vec() })
    }
}


fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn record_key(namespace_id: u64, record_id: u64) -> Result<Vec<u8>, DbError> {
    if namespace_id == 0 || record_id == 0 {
        return Err(DbError::Constraint("EF7 namespace and record IDs must be non-zero".into()));
    }
    let mut key = vec![KEY_RECORD];
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(&record_id.to_be_bytes());
    Ok(key)
}

fn uuid_key(namespace_id: u64, uuid: &[u8; 16]) -> Result<Vec<u8>, DbError> {
    if namespace_id == 0 {
        return Err(DbError::Constraint("EF7 namespace ID must be non-zero".into()));
    }
    let mut key = vec![KEY_PLAYER_UUID];
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(uuid);
    Ok(key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmberForge7Stats {
    pub allocated_pages: u64,
    pub free_pages: u64,
    pub free_extent_count: usize,
    pub next_page_id: u64,
}

#[derive(Debug, Clone, Copy)]
struct RecordDescriptor {
    start_page: u64,
    page_count: u32,
    total_len: u64,
    checksum: u64,
}

impl RecordDescriptor {
    fn encode(self) -> Result<Vec<u8>, DbError> {
        if self.page_count == 0 || self.page_count as usize > MAX_RECORD_PAGES
            || self.total_len as usize > MAX_RECORD_BYTES
        {
            return Err(DbError::Storage("invalid EF7 record descriptor bounds".into()));
        }
        let mut out = Vec::with_capacity(33);
        out.extend_from_slice(DESCRIPTOR_MAGIC);
        out.push(FORMAT_VERSION);
        out.extend_from_slice(&self.start_page.to_le_bytes());
        out.extend_from_slice(&self.page_count.to_le_bytes());
        out.extend_from_slice(&self.total_len.to_le_bytes());
        out.extend_from_slice(&self.checksum.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != 33 || &bytes[..4] != DESCRIPTOR_MAGIC || bytes[4] != FORMAT_VERSION {
            return Err(DbError::Storage("invalid EF7 record descriptor header or length".into()));
        }
        let value = Self {
            start_page: u64::from_le_bytes(bytes[5..13].try_into().unwrap()),
            page_count: u32::from_le_bytes(bytes[13..17].try_into().unwrap()),
            total_len: u64::from_le_bytes(bytes[17..25].try_into().unwrap()),
            checksum: u64::from_le_bytes(bytes[25..33].try_into().unwrap()),
        };
        if value.page_count == 0 || value.page_count as usize > MAX_RECORD_PAGES
            || value.total_len > MAX_RECORD_BYTES as u64
        {
            return Err(DbError::Storage("EF7 record descriptor exceeds supported bounds".into()));
        }
        let expected_pages = ((value.total_len as usize).max(1) + CHUNK_CAPACITY - 1) / CHUNK_CAPACITY;
        if expected_pages != value.page_count as usize {
            return Err(DbError::Storage("EF7 record descriptor page count does not match payload length".into()));
        }
        Ok(value)
    }
}

/// Minecraft/KSL-optimized persistent records over one CoW B+ tree and a
/// checksummed, contiguous overflow-page area. One instance owns its sidecar.
pub struct EmberForge7Store {
    pages: ExtentFile,
    wal_path: PathBuf,
    _lock_file: File,
}

impl EmberForge7Store {
    /// Opens the EF7 sidecar for a database path. Cache capacity is in pages.
    pub fn open(path: impl AsRef<Path>, cache_capacity_pages: usize) -> Result<Self, DbError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(storage_error)?;
            }
        }
        let lock_file = OpenOptions::new().create(true).read(true).write(true)
            .open(sidecar(path, ".ef7.lock")).map_err(storage_error)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("EmberForge store is already open: {error}")))?;
        let pages_path = sidecar(path, ".ef7.pages");
        let wal_path = sidecar(path, ".ef7.wal");
        let mut pages = ExtentFile::open_with_cache_capacity(pages_path, cache_capacity_pages)?;
        page_tree::recover(&mut pages, &wal_path)?;
        page_tree::validate_tree(&mut pages)?;
        Ok(Self { pages, wal_path, _lock_file: lock_file })
    }


    /// Creates a namespace and its parent-scoped name index atomically.
    pub fn create_namespace(
        &mut self,
        id: u64,
        parent_id: Option<u64>,
        kind: EmberForge7NamespaceKind,
        name: &str,
    ) -> Result<EmberForge7Namespace, DbError> {
        let canonical = normalize_namespace_name(name)?;
        let namespace = EmberForge7Namespace { id, parent_id, kind, name: canonical };
        let metadata_key = namespace_key(id)?;
        let name_key = namespace_name_key(parent_id, &namespace.name)?;
        if let Some(parent) = parent_id {
            if self.get_namespace(parent)?.is_none() {
                return Err(DbError::Constraint("EF7 namespace parent does not exist".into()));
            }
        }
        if page_tree::get(&mut self.pages, &metadata_key)?.is_some() {
            return Err(DbError::Constraint("EF7 namespace ID already exists".into()));
        }
        if page_tree::get(&mut self.pages, &name_key)?.is_some() {
            return Err(DbError::Constraint("EF7 namespace name already exists under this parent".into()));
        }
        page_tree::apply_mutations(
            &mut self.pages, &self.wal_path, &[],
            &[(metadata_key, namespace.encode()?), (name_key, id.to_be_bytes().to_vec())],
        )?;
        Ok(namespace)
    }

    pub fn get_namespace(&mut self, id: u64) -> Result<Option<EmberForge7Namespace>, DbError> {
        let key = namespace_key(id)?;
        let Some(value) = page_tree::get(&mut self.pages, &key)? else { return Ok(None); };
        let namespace = EmberForge7Namespace::decode(&value)?;
        if namespace.id != id {
            return Err(DbError::Storage("EF7 namespace key and metadata ID disagree".into()));
        }
        Ok(Some(namespace))
    }

    pub fn find_namespace(
        &mut self,
        parent_id: Option<u64>,
        name: &str,
    ) -> Result<Option<EmberForge7Namespace>, DbError> {
        let key = namespace_name_key(parent_id, name)?;
        let Some(value) = page_tree::get(&mut self.pages, &key)? else { return Ok(None); };
        if value.len() != 8 {
            return Err(DbError::Storage("malformed EF7 namespace name index value".into()));
        }
        let id = u64::from_be_bytes(value[..8].try_into().unwrap());
        let Some(namespace) = self.get_namespace(id)? else {
            return Err(DbError::Storage("EF7 namespace name index points to missing metadata".into()));
        };
        if namespace.parent_id != parent_id || namespace.name != normalize_namespace_name(name)? {
            return Err(DbError::Storage("EF7 namespace name index does not match metadata".into()));
        }
        Ok(Some(namespace))
    }

    /// Returns namespaces in key order, bounded to 10,000 entries per call.
    pub fn list_namespaces(
        &mut self,
        parent_id: Option<u64>,
        limit: usize,
    ) -> Result<Vec<EmberForge7Namespace>, DbError> {
        if limit == 0 || limit > 10_000 {
            return Err(DbError::Constraint("EF7 namespace list limit must be 1..=10000".into()));
        }
        let mut prefix = vec![KEY_NAMESPACE_NAME];
        prefix.extend_from_slice(&parent_id.unwrap_or(0).to_be_bytes());
        let entries = page_tree::scan_range_limited(
            &mut self.pages, &prefix, &prefix_upper_key(&prefix), Some(limit),
        )?;
        let mut result = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if key.len() <= prefix.len() || value.len() != 8 {
                return Err(DbError::Storage("malformed EF7 namespace name index entry".into()));
            }
            let id = u64::from_be_bytes(value[..8].try_into().unwrap());
            let namespace = self.get_namespace(id)?
                .ok_or_else(|| DbError::Storage("EF7 namespace index points to missing metadata".into()))?;
            if namespace.parent_id != parent_id || key != namespace_name_key(parent_id, &namespace.name)? {
                return Err(DbError::Storage("EF7 namespace index key does not match metadata".into()));
            }
            result.push(namespace);
        }
        Ok(result)
    }

    /// Deletes only empty namespaces. Children, records and player indexes
    /// must be removed first; namespace metadata and name index are one commit.
    pub fn delete_namespace(&mut self, id: u64) -> Result<bool, DbError> {
        let Some(namespace) = self.get_namespace(id)? else { return Ok(false); };
        let mut child_prefix = vec![KEY_NAMESPACE_NAME];
        child_prefix.extend_from_slice(&id.to_be_bytes());
        let mut record_prefix = vec![KEY_RECORD];
        record_prefix.extend_from_slice(&id.to_be_bytes());
        let mut uuid_prefix = vec![KEY_PLAYER_UUID];
        uuid_prefix.extend_from_slice(&id.to_be_bytes());
        let mut name_prefix = vec![KEY_PLAYER_NAME];
        name_prefix.extend_from_slice(&id.to_be_bytes());
        let mut identity_prefix = vec![KEY_PLAYER_ROW_META];
        identity_prefix.extend_from_slice(&id.to_be_bytes());
        let mut chunk_prefix = vec![KEY_CHUNK_DATA];
        chunk_prefix.extend_from_slice(&id.to_be_bytes());
        for prefix in [child_prefix, record_prefix, uuid_prefix, name_prefix, identity_prefix, chunk_prefix] {
            if !page_tree::scan_range_limited(
                &mut self.pages, &prefix, &prefix_upper_key(&prefix), Some(1),
            )?.is_empty() {
                return Err(DbError::Constraint("EF7 namespace is not empty".into()));
            }
        }
        let metadata_key = namespace_key(id)?;
        let name_key = namespace_name_key(namespace.parent_id, &namespace.name)?;
        let (removed, _) = page_tree::apply_mutations(
            &mut self.pages, &self.wal_path, &[metadata_key, name_key], &[],
        )?;
        if removed != 2 {
            return Err(DbError::Storage("EF7 namespace deletion did not remove both metadata keys".into()));
        }
        Ok(true)
    }


    /// Stores one Minecraft chunk payload by world namespace and signed chunk coordinates.
    /// The chunk descriptor itself is the persistent index entry, so replacement
    /// is a single tree publication and does not require a separate row-ID allocator.
    pub fn put_chunk(
        &mut self, world_namespace_id: u64, chunk_x: i32, chunk_z: i32, payload: &[u8],
    ) -> Result<(), DbError> {
        let key = chunk_key(world_namespace_id, chunk_x, chunk_z)?;
        let previous = page_tree::get(&mut self.pages, &key)?
            .map(|value| RecordDescriptor::decode(&value)).transpose()?;
        let next = self.write_payload(payload)?;
        page_tree::apply_mutations(
            &mut self.pages, &self.wal_path, &[], &[(key, next.encode()?)],
        )?;
        if let Some(old) = previous {
            if old.start_page != next.start_page {
                self.pages.release_extent(old.start_page, old.page_count as u64)?;
            }
        }
        Ok(())
    }

    pub fn get_chunk(
        &mut self, world_namespace_id: u64, chunk_x: i32, chunk_z: i32,
    ) -> Result<Option<Vec<u8>>, DbError> {
        let key = chunk_key(world_namespace_id, chunk_x, chunk_z)?;
        let Some(value) = page_tree::get(&mut self.pages, &key)? else { return Ok(None); };
        self.read_payload(RecordDescriptor::decode(&value)?).map(Some)
    }

    pub fn delete_chunk(
        &mut self, world_namespace_id: u64, chunk_x: i32, chunk_z: i32,
    ) -> Result<bool, DbError> {
        let key = chunk_key(world_namespace_id, chunk_x, chunk_z)?;
        let Some(value) = page_tree::get(&mut self.pages, &key)? else { return Ok(false); };
        let descriptor = RecordDescriptor::decode(&value)?;
        let (removed, _) = page_tree::apply_mutations(&mut self.pages, &self.wal_path, &[key], &[])?;
        if removed != 1 {
            return Err(DbError::Storage("EF7 chunk deletion did not remove its index entry".into()));
        }
        self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        Ok(true)
    }

    /// Reads chunks in a bounded rectangular coordinate range, ordered by X then Z.
    /// X spans are capped to 4,096 columns and result limits to 10,000 chunks.
    pub fn scan_chunks(
        &mut self,
        world_namespace_id: u64,
        min_x: i32,
        min_z: i32,
        max_x: i32,
        max_z: i32,
        limit: usize,
    ) -> Result<Vec<(i32, i32, Vec<u8>)>, DbError> {
        if world_namespace_id == 0 || min_x > max_x || min_z > max_z
            || i64::from(max_x) - i64::from(min_x) > 4_095 || limit == 0 || limit > 10_000 {
            return Err(DbError::Constraint("invalid EF7 chunk scan range or limit".into()));
        }
        let mut result = Vec::new();
        for x in i64::from(min_x)..=i64::from(max_x) {
            if result.len() >= limit { break; }
            let lower = chunk_key(world_namespace_id, x as i32, min_z)?;
            let upper = chunk_key(world_namespace_id, x as i32, max_z)?;
            let remaining = limit - result.len();
            let entries = page_tree::scan_range_limited(&mut self.pages, &lower, &upper, Some(remaining))?;
            for (key, value) in entries {
                let (namespace, chunk_x, chunk_z) = decode_chunk_coordinates(&key)?;
                if namespace != world_namespace_id {
                    return Err(DbError::Storage("EF7 chunk scan crossed namespace boundary".into()));
                }
                let payload = self.read_payload(RecordDescriptor::decode(&value)?)?;
                result.push((chunk_x, chunk_z, payload));
            }
        }
        Ok(result)
    }

    /// Stores or replaces a record up to 64 MiB. Data pages become durable
    /// before the CoW descriptor root is published.
    pub fn put_record(&mut self, namespace_id: u64, record_id: u64, payload: &[u8]) -> Result<(), DbError> {
        let key = record_key(namespace_id, record_id)?;
        let old = self.pages_descriptor(&key)?;
        let descriptor = self.write_payload(payload)?;
        page_tree::apply_mutations(
            &mut self.pages, &self.wal_path, &[], &[(key, descriptor.encode()?)],
        )?;
        if let Some(previous) = old {
            if previous.start_page != descriptor.start_page {
                self.pages.release_extent(previous.start_page, previous.page_count as u64)?;
            }
        }
        Ok(())
    }

    /// Reads and verifies a complete record. Returns None when it is absent.
    pub fn get_record(&mut self, namespace_id: u64, record_id: u64) -> Result<Option<Vec<u8>>, DbError> {
        let key = record_key(namespace_id, record_id)?;
        let Some(descriptor) = self.pages_descriptor(&key)? else { return Ok(None); };
        self.read_payload(descriptor).map(Some)
    }

    /// Deletes a record. Its pages are reclaimed only after the descriptor
    /// removal is durably published.
    pub fn delete_record(&mut self, namespace_id: u64, record_id: u64) -> Result<bool, DbError> {
        let key = record_key(namespace_id, record_id)?;
        let Some(descriptor) = self.pages_descriptor(&key)? else { return Ok(false); };
        let (removed, _) = page_tree::apply_mutations(&mut self.pages, &self.wal_path, &[key], &[])?;
        if removed != 1 {
            return Err(DbError::Storage("EF7 record disappeared during locked deletion".into()));
        }
        self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        Ok(true)
    }


    /// Stores raw plugin/player bytes and atomically maintains UUID + row identity.
    pub fn put_player(
        &mut self, namespace_id: u64, row_id: u64, uuid: [u8; 16], payload: &[u8],
    ) -> Result<(), DbError> {
        self.put_player_with_identity(namespace_id, row_id, uuid, None, payload)
    }

    /// Stores a typed Minecraft profile and indexes it by UUID and normalized name.
    pub fn put_player_profile(
        &mut self,
        namespace_id: u64,
        row_id: u64,
        profile: &EmberForge7PlayerProfile,
    ) -> Result<(), DbError> {
        let encoded = profile.encode()?;
        self.put_player_with_identity(namespace_id, row_id, profile.uuid,
            Some(&profile.username), &encoded)
    }

    /// UUID lookup verifies the player identity metadata before returning payload.
    pub fn get_player(
        &mut self, namespace_id: u64, uuid: [u8; 16],
    ) -> Result<Option<(u64, Vec<u8>)>, DbError> {
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? else { return Ok(None); };
        if value.len() != 8 {
            return Err(DbError::Storage("malformed EF7 UUID index value".into()));
        }
        let row_id = u64::from_be_bytes(value[..8].try_into().unwrap());
        if row_id == 0 {
            return Err(DbError::Storage("EF7 UUID index contains reserved row ID zero".into()));
        }
        let identity = self.player_identity_for_row(namespace_id, row_id)?
            .ok_or_else(|| DbError::Storage("EF7 UUID index points to a row without player identity".into()))?;
        if identity.uuid != uuid {
            return Err(DbError::Storage("EF7 UUID index disagrees with row identity metadata".into()));
        }
        let Some(payload) = self.get_record(namespace_id, row_id)? else {
            return Err(DbError::Storage("EF7 UUID index points to a missing player record".into()));
        };
        Ok(Some((row_id, payload)))
    }

    pub fn get_player_profile(
        &mut self, namespace_id: u64, uuid: [u8; 16],
    ) -> Result<Option<(u64, EmberForge7PlayerProfile)>, DbError> {
        let Some((row_id, payload)) = self.get_player(namespace_id, uuid)? else { return Ok(None); };
        let profile = EmberForge7PlayerProfile::decode(&payload)?;
        if profile.uuid != uuid {
            return Err(DbError::Storage("EF7 profile UUID does not match its index".into()));
        }
        let identity = self.player_identity_for_row(namespace_id, row_id)?
            .ok_or_else(|| DbError::Storage("EF7 player profile identity is missing".into()))?;
        if identity.username.as_deref() != Some(normalize_player_name(&profile.username)?.as_str()) {
            return Err(DbError::Storage("EF7 profile username does not match its name index metadata".into()));
        }
        Ok(Some((row_id, profile)))
    }

    /// Case-insensitive name lookup; profile and identity checks guard stale indexes.
    pub fn get_player_profile_by_name(
        &mut self, namespace_id: u64, username: &str,
    ) -> Result<Option<(u64, EmberForge7PlayerProfile)>, DbError> {
        let normalized = normalize_player_name(username)?;
        let key = player_name_key(namespace_id, &normalized)?;
        let Some(value) = page_tree::get(&mut self.pages, &key)? else { return Ok(None); };
        if value.len() != 8 {
            return Err(DbError::Storage("malformed EF7 player-name index value".into()));
        }
        let row_id = u64::from_be_bytes(value[..8].try_into().unwrap());
        let identity = self.player_identity_for_row(namespace_id, row_id)?
            .ok_or_else(|| DbError::Storage("EF7 player-name index points to missing identity".into()))?;
        if identity.username.as_deref() != Some(normalized.as_str()) {
            return Err(DbError::Storage("EF7 player-name index disagrees with row identity".into()));
        }
        let payload = self.get_record(namespace_id, row_id)?
            .ok_or_else(|| DbError::Storage("EF7 player-name index points to missing payload".into()))?;
        let profile = EmberForge7PlayerProfile::decode(&payload)?;
        if profile.uuid != identity.uuid || normalize_player_name(&profile.username)? != normalized {
            return Err(DbError::Storage("EF7 profile disagrees with its player indexes".into()));
        }
        Ok(Some((row_id, profile)))
    }


    /// Scans player profiles by UUID in bytewise order. The optional cursor is
    /// exclusive, making bounded pagination stable across calls.
    pub fn scan_player_profiles(
        &mut self,
        namespace_id: u64,
        after_uuid: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<Vec<(u64, EmberForge7PlayerProfile)>, DbError> {
        if limit == 0 || limit > 10_000 {
            return Err(DbError::Constraint("EF7 player scan limit must be 1..=10000".into()));
        }
        let mut prefix = vec![KEY_PLAYER_UUID];
        prefix.extend_from_slice(&namespace_id.to_be_bytes());
        if namespace_id == 0 {
            return Err(DbError::Constraint("EF7 namespace ID must be non-zero".into()));
        }
        let mut lower = prefix.clone();
        if let Some(cursor) = after_uuid {
            let mut cursor_key = uuid_key(namespace_id, &cursor)?;
            let mut advanced = false;
            for index in (9..25).rev() {
                if cursor_key[index] != 0xff {
                    cursor_key[index] += 1;
                    cursor_key.truncate(index + 1);
                    lower = cursor_key;
                    advanced = true;
                    break;
                }
            }
            if !advanced { return Ok(Vec::new()); }
        }
        let entries = page_tree::scan_range_limited(
            &mut self.pages, &lower, &prefix_upper_key(&prefix), Some(limit),
        )?;
        let mut result = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if key.len() != 25 || value.len() != 8 {
                return Err(DbError::Storage("malformed EF7 UUID index entry during scan".into()));
            }
            let uuid: [u8; 16] = key[9..25].try_into().unwrap();
            let row_id = u64::from_be_bytes(value[..8].try_into().unwrap());
            let Some((verified_row, profile)) = self.get_player_profile(namespace_id, uuid)? else {
                return Err(DbError::Storage("EF7 UUID index entry disappeared during scan".into()));
            };
            if verified_row != row_id {
                return Err(DbError::Storage("EF7 UUID scan row ID disagrees with profile lookup".into()));
            }
            result.push((row_id, profile));
        }
        Ok(result)
    }

    /// Prefix search over the persistent, case-insensitive player-name index.
    pub fn search_player_profiles_by_name(
        &mut self,
        namespace_id: u64,
        name_prefix: &str,
        limit: usize,
    ) -> Result<Vec<(u64, EmberForge7PlayerProfile)>, DbError> {
        if namespace_id == 0 || limit == 0 || limit > 10_000
            || name_prefix.len() > 16 || !name_prefix.is_ascii()
            || !name_prefix.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(DbError::Constraint("invalid EF7 player-name prefix or limit".into()));
        }
        let normalized = name_prefix.to_ascii_lowercase();
        let mut prefix = vec![KEY_PLAYER_NAME];
        prefix.extend_from_slice(&namespace_id.to_be_bytes());
        prefix.extend_from_slice(normalized.as_bytes());
        let entries = page_tree::scan_range_limited(
            &mut self.pages, &prefix, &prefix_upper_key(&prefix), Some(limit),
        )?;
        let mut result = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if key.len() < 10 || key.len() > 25 || value.len() != 8 {
                return Err(DbError::Storage("malformed EF7 player-name index entry during scan".into()));
            }
            let name = std::str::from_utf8(&key[9..])
                .map_err(|_| DbError::Storage("EF7 player-name index contains invalid UTF-8".into()))?;
            let row_id = u64::from_be_bytes(value[..8].try_into().unwrap());
            let identity = self.player_identity_for_row(namespace_id, row_id)?
                .ok_or_else(|| DbError::Storage("EF7 player-name index points to missing identity".into()))?;
            if identity.username.as_deref() != Some(name) || !name.starts_with(&normalized) {
                return Err(DbError::Storage("EF7 player-name prefix index does not match row identity".into()));
            }
            let payload = self.get_record(namespace_id, row_id)?
                .ok_or_else(|| DbError::Storage("EF7 player-name index points to missing payload".into()))?;
            let profile = EmberForge7PlayerProfile::decode(&payload)?;
            if profile.uuid != identity.uuid || normalize_player_name(&profile.username)? != name {
                return Err(DbError::Storage("EF7 player profile is inconsistent with its name index".into()));
            }
            result.push((row_id, profile));
        }
        Ok(result)
    }

    /// Deletes a player row, UUID mapping, optional name mapping and identity
    /// metadata in one root publication.
    pub fn delete_player(
        &mut self, namespace_id: u64, row_id: u64, uuid: [u8; 16],
    ) -> Result<bool, DbError> {
        let row_key = record_key(namespace_id, row_id)?;
        let Some(descriptor) = self.pages_descriptor(&row_key)? else { return Ok(false); };
        let identity = self.player_identity_for_row(namespace_id, row_id)?
            .ok_or_else(|| DbError::Storage("EF7 player record has no UUID identity".into()))?;
        if identity.uuid != uuid {
            return Err(DbError::Constraint("supplied UUID does not identify the requested player row".into()));
        }
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        let lookup = page_tree::get(&mut self.pages, &lookup_key)?
            .ok_or_else(|| DbError::Storage("EF7 player identity has no UUID index entry".into()))?;
        if lookup.len() != 8 || u64::from_be_bytes(lookup[..8].try_into().unwrap()) != row_id {
            return Err(DbError::Storage("EF7 UUID mapping does not point to the requested row".into()));
        }
        let mut removals = vec![row_key, lookup_key];
        let identity_key = player_identity_key(namespace_id, row_id)?;
        if page_tree::get(&mut self.pages, &identity_key)?.is_some() {
            removals.push(identity_key);
        }
        if let Some(name) = identity.username {
            let name_key = player_name_key(namespace_id, &name)?;
            let value = page_tree::get(&mut self.pages, &name_key)?
                .ok_or_else(|| DbError::Storage("EF7 player-name identity has no index entry".into()))?;
            if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != row_id {
                return Err(DbError::Storage("EF7 player-name index does not point to the requested row".into()));
            }
            removals.push(name_key);
        }
        let (removed, _) = page_tree::apply_mutations(&mut self.pages, &self.wal_path, &removals, &[])?;
        if removed != removals.len() {
            return Err(DbError::Storage("EF7 player deletion did not remove all identity keys".into()));
        }
        self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        Ok(true)
    }

    fn put_player_with_identity(
        &mut self,
        namespace_id: u64,
        row_id: u64,
        uuid: [u8; 16],
        username: Option<&str>,
        payload: &[u8],
    ) -> Result<(), DbError> {
        if payload.len() > MAX_RECORD_BYTES {
            return Err(DbError::Constraint("EF7 player record exceeds 64 MiB".into()));
        }
        let canonical_name = username.map(normalize_player_name).transpose()?;
        let row_key = record_key(namespace_id, row_id)?;
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        let identity_key = player_identity_key(namespace_id, row_id)?;
        let name_key = canonical_name.as_deref()
            .map(|name| player_name_key(namespace_id, name)).transpose()?;
        let previous_descriptor = self.pages_descriptor(&row_key)?;
        let previous_identity = self.player_identity_for_row(namespace_id, row_id)?;

        if previous_descriptor.is_some() && previous_identity.is_none() {
            return Err(DbError::Constraint("EF7 row ID is already used by a non-player record".into()));
        }
        if let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? {
            if value.len() != 8 {
                return Err(DbError::Storage("malformed EF7 UUID index value".into()));
            }
            if u64::from_be_bytes(value[..8].try_into().unwrap()) != row_id {
                return Err(DbError::Constraint("player UUID is already assigned to another record".into()));
            }
        }
        if let Some(key) = &name_key {
            if let Some(value) = page_tree::get(&mut self.pages, key)? {
                if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != row_id {
                    return Err(DbError::Constraint("Minecraft player name is already assigned in this namespace".into()));
                }
            }
        }

        let next_identity = PlayerIdentity { uuid, username: canonical_name.clone() };
        let mut removals = Vec::with_capacity(2);
        if let Some(previous) = &previous_identity {
            if previous.uuid != uuid {
                let old_key = uuid_key(namespace_id, &previous.uuid)?;
                let old_value = page_tree::get(&mut self.pages, &old_key)?
                    .ok_or_else(|| DbError::Storage("previous EF7 player UUID index is missing".into()))?;
                if old_value.len() != 8 || u64::from_be_bytes(old_value[..8].try_into().unwrap()) != row_id {
                    return Err(DbError::Storage("previous EF7 player UUID index is inconsistent".into()));
                }
                removals.push(old_key);
            }
            if previous.username != canonical_name {
                if let Some(old_name) = &previous.username {
                    let old_key = player_name_key(namespace_id, old_name)?;
                    let old_value = page_tree::get(&mut self.pages, &old_key)?
                        .ok_or_else(|| DbError::Storage("previous EF7 player-name index is missing".into()))?;
                    if old_value.len() != 8 || u64::from_be_bytes(old_value[..8].try_into().unwrap()) != row_id {
                        return Err(DbError::Storage("previous EF7 player-name index is inconsistent".into()));
                    }
                    removals.push(old_key);
                }
            }
        }

        let next_descriptor = self.write_payload(payload)?;
        let mut upserts = vec![
            (row_key, next_descriptor.encode()?),
            (lookup_key, row_id.to_be_bytes().to_vec()),
            (identity_key, next_identity.encode()?),
        ];
        if let Some(key) = name_key {
            upserts.push((key, row_id.to_be_bytes().to_vec()));
        }
        page_tree::apply_mutations(&mut self.pages, &self.wal_path, &removals, &upserts)?;
        if let Some(previous) = previous_descriptor {
            if previous.start_page != next_descriptor.start_page {
                self.pages.release_extent(previous.start_page, previous.page_count as u64)?;
            }
        }
        Ok(())
    }

    /// Reads the identity sidecar. If an older EF7 file predates row metadata,
    /// the method performs a bounded one-time reverse UUID scan for compatibility.
    fn player_identity_for_row(
        &mut self, namespace_id: u64, row_id: u64,
    ) -> Result<Option<PlayerIdentity>, DbError> {
        let key = player_identity_key(namespace_id, row_id)?;
        if let Some(value) = page_tree::get(&mut self.pages, &key)? {
            return PlayerIdentity::decode(&value).map(Some);
        }
        if self.pages_descriptor(&record_key(namespace_id, row_id)?)?.is_none() {
            return Ok(None);
        }
        let mut prefix = vec![KEY_PLAYER_UUID];
        prefix.extend_from_slice(&namespace_id.to_be_bytes());
        let entries = page_tree::scan_range_limited(
            &mut self.pages, &prefix, &prefix_upper_key(&prefix), Some(100_001),
        )?;
        if entries.len() > 100_000 {
            return Err(DbError::Constraint("legacy EF7 UUID reverse lookup exceeded 100,000 entries; run the EF7 identity migration".into()));
        }
        let mut found = None;
        for (key, value) in entries {
            if value.len() != 8 {
                return Err(DbError::Storage("malformed legacy EF7 UUID index value".into()));
            }
            if u64::from_be_bytes(value[..8].try_into().unwrap()) == row_id {
                if key.len() != 25 {
                    return Err(DbError::Storage("malformed legacy EF7 UUID index key".into()));
                }
                if found.is_some() {
                    return Err(DbError::Storage("legacy EF7 row has multiple UUID index entries".into()));
                }
                found = Some(PlayerIdentity { uuid: key[9..25].try_into().unwrap(), username: None });
            }
        }
        Ok(found)
    }

    /// Applies a bounded player batch using a single CoW redo record/root publication.
    pub fn apply_player_batch(
        &mut self,
        upserts: &[(u64, u64, [u8; 16], Vec<u8>)],
        removals: &[(u64, u64, [u8; 16])],
    ) -> Result<(usize, usize), DbError> {
        if upserts.is_empty() && removals.is_empty() { return Ok((0, 0)); }
        const MAX_BATCH_PLAYERS: usize = 2_500;
        const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
        if upserts.len().saturating_add(removals.len()) > MAX_BATCH_PLAYERS {
            return Err(DbError::Constraint(format!("EF7 player batch exceeds {MAX_BATCH_PLAYERS} operations")));
        }

        let mut upsert_rows = BTreeSet::new();
        let mut upsert_uuids = BTreeSet::new();
        let mut total_bytes = 0usize;
        for (namespace_id, row_id, uuid, payload) in upserts {
            let row = record_key(*namespace_id, *row_id)?;
            let lookup = uuid_key(*namespace_id, uuid)?;
            if !upsert_rows.insert(row) || !upsert_uuids.insert(lookup) {
                return Err(DbError::Constraint("EF7 batch contains duplicate player row IDs or UUIDs".into()));
            }
            if payload.len() > MAX_RECORD_BYTES {
                return Err(DbError::Constraint("EF7 player record exceeds 64 MiB".into()));
            }
            total_bytes = total_bytes.checked_add(payload.len())
                .ok_or_else(|| DbError::Constraint("EF7 batch byte count overflow".into()))?;
            if total_bytes > MAX_BATCH_BYTES {
                return Err(DbError::Constraint("EF7 batch exceeds 256 MiB of payload data".into()));
            }
        }

        let mut removal_rows = BTreeSet::new();
        let mut removal_uuids = BTreeSet::new();
        let mut removal_keys = Vec::with_capacity(removals.len() * 3);
        let mut removed_extents = Vec::with_capacity(removals.len());
        for (namespace_id, row_id, uuid) in removals {
            let row_key = record_key(*namespace_id, *row_id)?;
            let lookup_key = uuid_key(*namespace_id, uuid)?;
            if !removal_rows.insert(row_key.clone()) || !removal_uuids.insert(lookup_key.clone())
                || upsert_rows.contains(&row_key) || upsert_uuids.contains(&lookup_key) {
                return Err(DbError::Constraint("EF7 batch contains duplicate or overlapping player operations".into()));
            }
            let Some(descriptor) = self.pages_descriptor(&row_key)? else {
                if let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? {
                    if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) == *row_id {
                        return Err(DbError::Storage("EF7 UUID index references a missing player record".into()));
                    }
                }
                continue;
            };
            let identity = self.player_identity_for_row(*namespace_id, *row_id)?
                .ok_or_else(|| DbError::Storage("EF7 player row has no identity metadata".into()))?;
            if identity.uuid != *uuid {
                return Err(DbError::Constraint("EF7 batch removal UUID does not match row identity".into()));
            }
            let value = page_tree::get(&mut self.pages, &lookup_key)?
                .ok_or_else(|| DbError::Storage("EF7 player UUID index is missing".into()))?;
            if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != *row_id {
                return Err(DbError::Storage("EF7 player UUID mapping is inconsistent".into()));
            }
            removed_extents.push(descriptor);
            removal_keys.push(row_key);
            removal_keys.push(lookup_key);
            let identity_key = player_identity_key(*namespace_id, *row_id)?;
            if page_tree::get(&mut self.pages, &identity_key)?.is_some() {
                removal_keys.push(identity_key);
            }
            if let Some(name) = identity.username {
                let name_key = player_name_key(*namespace_id, &name)?;
                let name_value = page_tree::get(&mut self.pages, &name_key)?
                    .ok_or_else(|| DbError::Storage("EF7 player-name index is missing".into()))?;
                if name_value.len() != 8 || u64::from_be_bytes(name_value[..8].try_into().unwrap()) != *row_id {
                    return Err(DbError::Storage("EF7 player-name index is inconsistent".into()));
                }
                removal_keys.push(name_key);
            }
        }

        let mut upsert_entries = Vec::with_capacity(upserts.len() * 3);
        let mut replaced_extents = Vec::with_capacity(upserts.len());
        for (namespace_id, row_id, uuid, payload) in upserts {
            let row_key = record_key(*namespace_id, *row_id)?;
            let lookup_key = uuid_key(*namespace_id, uuid)?;
            let identity_key = player_identity_key(*namespace_id, *row_id)?;
            if let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? {
                if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != *row_id {
                    return Err(DbError::Constraint("player UUID is already assigned to another record".into()));
                }
            }
            let previous = self.pages_descriptor(&row_key)?;
            let old_identity = self.player_identity_for_row(*namespace_id, *row_id)?;
            if previous.is_some() && old_identity.is_none() {
                return Err(DbError::Constraint("EF7 row ID is already used by a non-player record".into()));
            }
            if let Some(old) = &old_identity {
                if old.uuid != *uuid {
                    let old_key = uuid_key(*namespace_id, &old.uuid)?;
                    let old_value = page_tree::get(&mut self.pages, &old_key)?
                        .ok_or_else(|| DbError::Storage("previous EF7 player UUID index is missing".into()))?;
                    if old_value.len() != 8 || u64::from_be_bytes(old_value[..8].try_into().unwrap()) != *row_id {
                        return Err(DbError::Storage("previous EF7 player UUID index is inconsistent".into()));
                    }
                    removal_keys.push(old_key);
                }
                if let Some(name) = &old.username {
                    let old_name_key = player_name_key(*namespace_id, name)?;
                    let old_name_value = page_tree::get(&mut self.pages, &old_name_key)?
                        .ok_or_else(|| DbError::Storage("previous EF7 player-name index is missing".into()))?;
                    if old_name_value.len() != 8 || u64::from_be_bytes(old_name_value[..8].try_into().unwrap()) != *row_id {
                        return Err(DbError::Storage("previous EF7 player-name index is inconsistent".into()));
                    }
                    removal_keys.push(old_name_key);
                }
            }
            let next = self.write_payload(payload)?;
            upsert_entries.push((row_key, next.encode()?));
            upsert_entries.push((lookup_key, row_id.to_be_bytes().to_vec()));
            upsert_entries.push((identity_key, PlayerIdentity { uuid: *uuid, username: None }.encode()?));
            replaced_extents.push((previous, next));
        }

        let mut unique_removals = BTreeSet::new();
        for key in &removal_keys {
            if !unique_removals.insert(key.clone()) {
                return Err(DbError::Constraint("EF7 batch produced duplicate index removals".into()));
            }
        }
        let upsert_key_set = upsert_entries.iter().map(|(key, _)| key.clone()).collect::<BTreeSet<_>>();
        if removal_keys.iter().any(|key| upsert_key_set.contains(key)) {
            return Err(DbError::Constraint("EF7 batch attempts to remove and replace the same index key".into()));
        }

        let counts = page_tree::apply_mutations(&mut self.pages, &self.wal_path, &removal_keys, &upsert_entries)?;
        for descriptor in removed_extents {
            self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        }
        for (previous, next) in replaced_extents {
            if let Some(previous) = previous {
                if previous.start_page != next.start_page {
                    self.pages.release_extent(previous.start_page, previous.page_count as u64)?;
                }
            }
        }
        Ok(counts)
    }


    /// Atomically upserts/removes a bounded set of world chunks through one
    /// CoW redo/root publication. Payload extents are reclaimed after commit.
    pub fn apply_chunk_batch(
        &mut self,
        upserts: &[(i32, i32, Vec<u8>)],
        removals: &[(i32, i32)],
    ) -> Result<(usize, usize), DbError> {
        const MAX_CHUNK_OPS: usize = 2_500;
        const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
        if upserts.is_empty() && removals.is_empty() { return Ok((0, 0)); }
        if upserts.len().saturating_add(removals.len()) > MAX_CHUNK_OPS {
            return Err(DbError::Constraint(format!("EF7 chunk batch exceeds {MAX_CHUNK_OPS} operations")));
        }
        let mut upsert_keys = BTreeSet::new();
        let mut total_bytes = 0usize;
        for (x, z, payload) in upserts {
            let key = chunk_key(1, *x, *z)?;
            if !upsert_keys.insert(key) {
                return Err(DbError::Constraint("EF7 chunk batch contains duplicate coordinates".into()));
            }
            if payload.len() > MAX_RECORD_BYTES {
                return Err(DbError::Constraint("EF7 chunk payload exceeds 64 MiB".into()));
            }
            total_bytes = total_bytes.checked_add(payload.len())
                .ok_or_else(|| DbError::Constraint("EF7 chunk batch byte count overflow".into()))?;
            if total_bytes > MAX_BATCH_BYTES {
                return Err(DbError::Constraint("EF7 chunk batch exceeds 256 MiB of payload data".into()));
            }
        }
        let mut removal_keys = Vec::with_capacity(removals.len());
        let mut removal_set = BTreeSet::new();
        let mut removed_extents = Vec::with_capacity(removals.len());
        for (x, z) in removals {
            let key = chunk_key(1, *x, *z)?;
            if !removal_set.insert(key.clone()) || upsert_keys.contains(&key) {
                return Err(DbError::Constraint("EF7 chunk batch contains duplicate or overlapping coordinates".into()));
            }
            if let Some(value) = page_tree::get(&mut self.pages, &key)? {
                removed_extents.push(RecordDescriptor::decode(&value)?);
                removal_keys.push(key);
            }
        }

        let mut upsert_entries = Vec::with_capacity(upserts.len());
        let mut replaced_extents = Vec::with_capacity(upserts.len());
        for (x, z, payload) in upserts {
            let key = chunk_key(1, *x, *z)?;
            let previous = page_tree::get(&mut self.pages, &key)?
                .map(|value| RecordDescriptor::decode(&value)).transpose()?;
            let next = self.write_payload(payload)?;
            upsert_entries.push((key, next.encode()?));
            replaced_extents.push((previous, next));
        }
        let counts = page_tree::apply_mutations(
            &mut self.pages, &self.wal_path, &removal_keys, &upsert_entries,
        )?;
        for descriptor in removed_extents {
            self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        }
        for (previous, next) in replaced_extents {
            if let Some(previous) = previous {
                if previous.start_page != next.start_page {
                    self.pages.release_extent(previous.start_page, previous.page_count as u64)?;
                }
            }
        }
        Ok(counts)
    }

    pub fn stats(&self) -> EmberForge7Stats {
        let stats = self.pages.stats();
        EmberForge7Stats {
            allocated_pages: stats.allocated_pages,
            free_pages: stats.free_pages,
            free_extent_count: stats.free_extent_count,
            next_page_id: stats.next_page_id,
        }
    }

    fn pages_descriptor(&mut self, key: &[u8]) -> Result<Option<RecordDescriptor>, DbError> {
        page_tree::get(&mut self.pages, key)?
            .map(|value| RecordDescriptor::decode(&value))
            .transpose()
    }

    fn write_payload(&mut self, payload: &[u8]) -> Result<RecordDescriptor, DbError> {
        if payload.len() > MAX_RECORD_BYTES {
            return Err(DbError::Constraint("EF7 record exceeds the 64 MiB limit".into()));
        }
        let page_count = (payload.len().max(1) + CHUNK_CAPACITY - 1) / CHUNK_CAPACITY;
        if page_count > MAX_RECORD_PAGES {
            return Err(DbError::Constraint("EF7 record requires too many pages".into()));
        }
        let page_count_u64 = u64::try_from(page_count)
            .map_err(|_| DbError::Constraint("EF7 page count conversion overflow".into()))?;
        let start = self.pages.allocate_extent(page_count_u64)?;
        let record_checksum = checksum(payload);
        let mut page_payloads = Vec::with_capacity(page_count);
        for index in 0..page_count {
            let offset = index * CHUNK_CAPACITY;
            let end = (offset + CHUNK_CAPACITY).min(payload.len());
            let chunk = if offset < payload.len() { &payload[offset..end] } else { &[] };
            let mut page = Vec::with_capacity(CHUNK_HEADER + chunk.len());
            page.extend_from_slice(CHUNK_MAGIC);
            page.push(FORMAT_VERSION);
            page.extend_from_slice(&(index as u32).to_le_bytes());
            page.extend_from_slice(&(page_count as u32).to_le_bytes());
            page.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            page.extend_from_slice(&record_checksum.to_le_bytes());
            page.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            page.extend_from_slice(&checksum(chunk).to_le_bytes());
            page.extend_from_slice(chunk);
            page_payloads.push(page);
        }
        self.pages.write_pages(start, &page_payloads)?;
        Ok(RecordDescriptor {
            start_page: start,
            page_count: page_count as u32,
            total_len: payload.len() as u64,
            checksum: record_checksum,
        })
    }

    fn read_payload(&mut self, descriptor: RecordDescriptor) -> Result<Vec<u8>, DbError> {
        let total_len = usize::try_from(descriptor.total_len)
            .map_err(|_| DbError::Storage("EF7 record length does not fit this platform".into()))?;
        let mut out = Vec::with_capacity(total_len);
        for index in 0..descriptor.page_count {
            let page_id = descriptor.start_page.checked_add(index as u64)
                .ok_or_else(|| DbError::Storage("EF7 page ID overflow".into()))?;
            let page = self.pages.read_page(page_id)?;
            if page.len() < CHUNK_HEADER || &page[..4] != CHUNK_MAGIC || page[4] != FORMAT_VERSION {
                return Err(DbError::Storage(format!("invalid EF7 chunk header at page {page_id}")));
            }
            let ordinal = u32::from_le_bytes(page[5..9].try_into().unwrap());
            let count = u32::from_le_bytes(page[9..13].try_into().unwrap());
            let length = u64::from_le_bytes(page[13..21].try_into().unwrap());
            let full_checksum = u64::from_le_bytes(page[21..29].try_into().unwrap());
            let chunk_len = u32::from_le_bytes(page[29..33].try_into().unwrap()) as usize;
            let chunk_checksum = u64::from_le_bytes(page[33..41].try_into().unwrap());
            if ordinal != index || count != descriptor.page_count || length != descriptor.total_len
                || full_checksum != descriptor.checksum || chunk_len > CHUNK_CAPACITY
                || page.len() != CHUNK_HEADER + chunk_len
                || checksum(&page[CHUNK_HEADER..]) != chunk_checksum
            {
                return Err(DbError::Storage(format!("EF7 chunk metadata or checksum mismatch at page {page_id}")));
            }
            out.extend_from_slice(&page[CHUNK_HEADER..]);
        }
        if out.len() != total_len || checksum(&out) != descriptor.checksum {
            return Err(DbError::Storage("EF7 record length or whole-record checksum mismatch".into()));
        }
        Ok(out)
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn storage_error(error: std::io::Error) -> DbError {
    DbError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("postemberdb-ef7-{label}-{}-{}",
            std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }

    fn cleanup(path: &Path) {
        for suffix in [".ef7.lock", ".ef7.pages", ".ef7.wal"] {
            let _ = fs::remove_file(sidecar(path, suffix));
        }
    }

    #[test]
    fn large_records_round_trip_and_reclaim_pages_after_delete() {
        let path = path("large-record");
        let payload = (0..180_000).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        {
            let mut store = EmberForge7Store::open(&path, 32).unwrap();
            store.put_record(1, 7, &payload).unwrap();
            assert_eq!(store.get_record(1, 7).unwrap(), Some(payload.clone()));
            assert!(store.stats().allocated_pages >= 45);
            assert!(store.delete_record(1, 7).unwrap());
            assert_eq!(store.get_record(1, 7).unwrap(), None);
            assert!(store.stats().free_pages >= 45);
        }
        {
            let mut store = EmberForge7Store::open(&path, 32).unwrap();
            assert_eq!(store.get_record(1, 7).unwrap(), None);
            let small = b"reused page range";
            store.put_record(1, 8, small).unwrap();
            assert_eq!(store.get_record(1, 8).unwrap(), Some(small.to_vec()));
        }
        cleanup(&path);
    }

    #[test]
    fn player_uuid_mapping_and_record_commit_together() {
        let path = path("player-uuid");
        let uuid = [0x5au8; 16];
        {
            let mut store = EmberForge7Store::open(&path, 16).unwrap();
            store.put_player(9, 41, uuid, b"player-data").unwrap();
            assert_eq!(store.get_player(9, uuid).unwrap(), Some((41, b"player-data".to_vec())));
            assert!(store.put_player(9, 42, uuid, b"collision").is_err());
            assert!(store.delete_player(9, 41, uuid).unwrap());
            assert_eq!(store.get_player(9, uuid).unwrap(), None);
        }
        cleanup(&path);
    }


    #[test]
    fn player_batch_is_atomic_bounded_and_survives_reopen() {
        let path = path("player-batch");
        let uuid_a = [0x11; 16];
        let uuid_b = [0x22; 16];
        {
            let mut store = EmberForge7Store::open(&path, 32).unwrap();
            assert_eq!(store.apply_player_batch(&[], &[]).unwrap(), (0, 0));
            let upserts = vec![
                (7, 101, uuid_a, b"player-a".to_vec()),
                (7, 102, uuid_b, vec![0x5a; 90_000]),
            ];
            assert_eq!(store.apply_player_batch(&upserts, &[]).unwrap(), (0, 6));
            assert_eq!(store.get_player(7, uuid_a).unwrap(), Some((101, b"player-a".to_vec())));
            assert_eq!(store.get_player(7, uuid_b).unwrap(), Some((102, vec![0x5a; 90_000])));
            let replacement = vec![(7, 102, uuid_b, b"updated-player-b".to_vec())];
            let removals = vec![(7, 101, uuid_a)];
            assert_eq!(store.apply_player_batch(&replacement, &removals).unwrap(), (3, 3));
            assert_eq!(store.get_player(7, uuid_a).unwrap(), None);
            assert_eq!(store.get_player(7, uuid_b).unwrap(), Some((102, b"updated-player-b".to_vec())));
            let before = store.stats();
            let duplicate = vec![
                (7, 201, [0x33; 16], b"one".to_vec()),
                (7, 201, [0x44; 16], b"two".to_vec()),
            ];
            assert!(store.apply_player_batch(&duplicate, &[]).is_err());
            assert_eq!(store.stats(), before);
        }
        {
            let mut reopened = EmberForge7Store::open(&path, 32).unwrap();
            assert_eq!(reopened.get_player(7, uuid_a).unwrap(), None);
            assert_eq!(reopened.get_player(7, uuid_b).unwrap(), Some((102, b"updated-player-b".to_vec())));
        }
        cleanup(&path);
    }


    #[test]
    fn namespaces_are_hierarchical_unique_and_deletable_only_when_empty() {
        let path = path("namespaces");
        let mut store = EmberForge7Store::open(&path, 16).unwrap();
        let root = store.create_namespace(1, None, EmberForge7NamespaceKind::Server, "Lobby").unwrap();
        assert_eq!(root.name, "lobby");
        let world = store.create_namespace(2, Some(1), EmberForge7NamespaceKind::World, "Survival_01").unwrap();
        assert_eq!(world.parent_id, Some(1));
        assert_eq!(store.find_namespace(Some(1), "SURVIVAL_01").unwrap(), Some(world.clone()));
        assert_eq!(store.list_namespaces(Some(1), 10).unwrap(), vec![world.clone()]);
        assert!(store.create_namespace(3, Some(1), EmberForge7NamespaceKind::Plugin, "survival_01").is_err());
        assert!(store.delete_namespace(1).is_err(), "a namespace with children cannot be removed");
        store.put_record(2, 100, b"world metadata").unwrap();
        assert!(store.delete_namespace(2).is_err(), "a namespace with records cannot be removed");
        assert!(store.delete_record(2, 100).unwrap());
        assert!(store.delete_namespace(2).unwrap());
        assert!(store.delete_namespace(1).unwrap());
        assert!(store.get_namespace(1).unwrap().is_none());
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn typed_player_profiles_rekey_uuid_and_name_indexes_atomically() {
        let path = path("typed-player");
        let first_uuid = [0x31; 16];
        let second_uuid = [0x32; 16];
        {
            let mut store = EmberForge7Store::open(&path, 24).unwrap();
            let first = EmberForge7PlayerProfile {
                uuid: first_uuid, username: "Steve".into(),
                first_joined_ms: 100, last_seen_ms: 200,
                playtime_ticks: 900, custom_data: vec![1, 2, 3, 4],
            };
            store.put_player_profile(7, 55, &first).unwrap();
            assert_eq!(store.get_player_profile(7, first_uuid).unwrap(), Some((55, first.clone())));
            assert_eq!(store.get_player_profile_by_name(7, "STEVE").unwrap(), Some((55, first)));

            let renamed = EmberForge7PlayerProfile {
                uuid: second_uuid, username: "Alex_2".into(),
                first_joined_ms: 100, last_seen_ms: 300,
                playtime_ticks: 1_200, custom_data: vec![9, 8, 7],
            };
            store.put_player_profile(7, 55, &renamed).unwrap();
            assert!(store.get_player_profile(7, first_uuid).unwrap().is_none());
            assert!(store.get_player_profile_by_name(7, "Steve").unwrap().is_none());
            assert_eq!(store.get_player_profile_by_name(7, "alex_2").unwrap(), Some((55, renamed.clone())));
            assert_eq!(store.get_player_profile(7, second_uuid).unwrap(), Some((55, renamed)));
            assert!(store.delete_player(7, 55, first_uuid).is_err());
            assert!(store.delete_player(7, 55, second_uuid).unwrap());
            assert!(store.get_player_profile_by_name(7, "Alex_2").unwrap().is_none());
        }
        {
            let mut reopened = EmberForge7Store::open(&path, 24).unwrap();
            assert!(reopened.get_player_profile(7, second_uuid).unwrap().is_none());
        }
        drop(());
        cleanup(&path);
    }

    #[test]
    fn player_identity_metadata_survives_raw_batch_update_and_uuid_reassignment() {
        let path = path("batch-rekey");
        let old_uuid = [0x41; 16];
        let new_uuid = [0x42; 16];
        {
            let mut store = EmberForge7Store::open(&path, 16).unwrap();
            let profile = EmberForge7PlayerProfile {
                uuid: old_uuid, username: "Builder".into(),
                first_joined_ms: 10, last_seen_ms: 20, playtime_ticks: 30,
                custom_data: vec![1],
            };
            store.put_player_profile(5, 77, &profile).unwrap();
            store.apply_player_batch(&[(5, 77, new_uuid, b"raw-data".to_vec())], &[]).unwrap();
            assert!(store.get_player(5, old_uuid).unwrap().is_none());
            assert_eq!(store.get_player(5, new_uuid).unwrap(), Some((77, b"raw-data".to_vec())));
            assert!(store.get_player_profile_by_name(5, "Builder").unwrap().is_none());
        }
        {
            let mut reopened = EmberForge7Store::open(&path, 16).unwrap();
            assert_eq!(reopened.get_player(5, new_uuid).unwrap(), Some((77, b"raw-data".to_vec())));
        }
        cleanup(&path);
    }


    #[test]
    fn chunk_store_orders_signed_coordinates_and_scans_bounded_regions() {
        let path = path("chunks");
        {
            let mut store = EmberForge7Store::open(&path, 16).unwrap();
            for (x, z, data) in [(-1, 2, b"negative-x".as_slice()), (0, 0, b"origin".as_slice()),
                (1, -2, b"positive-x".as_slice()), (-1, -3, b"negative-z".as_slice())] {
                store.put_chunk(44, x, z, data).unwrap();
            }
            assert_eq!(store.get_chunk(44, -1, 2).unwrap(), Some(b"negative-x".to_vec()));
            assert_eq!(store.get_chunk(44, 99, 99).unwrap(), None);
            let scan = store.scan_chunks(44, -1, -3, 1, 2, 10).unwrap();
            assert_eq!(scan.iter().map(|(x, z, _)| (*x, *z)).collect::<Vec<_>>(),
                vec![(-1, -3), (-1, 2), (0, 0), (1, -2)]);
            assert_eq!(store.scan_chunks(44, -1, -3, 1, 2, 2).unwrap().len(), 2);
            assert!(store.scan_chunks(44, 5, 0, 4, 1, 10).is_err());
            assert!(store.delete_chunk(44, 0, 0).unwrap());
            assert!(!store.delete_chunk(44, 0, 0).unwrap());
        }
        {
            let mut reopened = EmberForge7Store::open(&path, 16).unwrap();
            assert_eq!(reopened.get_chunk(44, -1, 2).unwrap(), Some(b"negative-x".to_vec()));
            assert_eq!(reopened.get_chunk(44, 0, 0).unwrap(), None);
        }
        cleanup(&path);
    }

    #[test]
    fn namespace_cannot_be_deleted_while_chunk_payloads_exist() {
        let path = path("namespace-chunk-guard");
        let mut store = EmberForge7Store::open(&path, 8).unwrap();
        store.create_namespace(88, None, EmberForge7NamespaceKind::World, "world").unwrap();
        store.put_chunk(88, -1, 2, b"chunk").unwrap();
        assert!(store.delete_namespace(88).is_err());
        assert!(store.delete_chunk(88, -1, 2).unwrap());
        assert!(store.delete_namespace(88).unwrap());
        drop(store);
        cleanup(&path);
    }


    #[test]
    fn player_profile_scans_support_bounded_uuid_pages_and_name_prefixes() {
        let path = path("player-scans");
        let mut store = EmberForge7Store::open(&path, 24).unwrap();
        for (n, name) in [(1u8, "AlexOne"), (2, "AlexTwo"), (3, "Steve"), (4, "AlexThree")] {
            let profile = EmberForge7PlayerProfile {
                uuid: [n; 16], username: name.into(), first_joined_ms: 1,
                last_seen_ms: 2, playtime_ticks: 3, custom_data: Vec::new(),
            };
            store.put_player_profile(12, u64::from(n), &profile).unwrap();
        }
        let first = store.scan_player_profiles(12, None, 2).unwrap();
        assert_eq!(first.iter().map(|(_, p)| p.uuid[0]).collect::<Vec<_>>(), vec![1, 2]);
        let second = store.scan_player_profiles(12, Some(first.last().unwrap().1.uuid), 2).unwrap();
        assert_eq!(second.iter().map(|(_, p)| p.uuid[0]).collect::<Vec<_>>(), vec![3, 4]);
        assert!(store.scan_player_profiles(12, Some([0xff; 16]), 2).unwrap().is_empty());
        let matches = store.search_player_profiles_by_name(12, "aLeX", 10).unwrap();
        assert_eq!(matches.len(), 3);
        assert_eq!(matches.iter().map(|(_, p)| p.username.as_str()).collect::<Vec<_>>(),
            vec!["AlexOne", "AlexThree", "AlexTwo"]);
        assert!(store.search_player_profiles_by_name(12, "", 0).is_err());
        drop(store);
        cleanup(&path);
    }


    #[test]
    fn chunk_batches_commit_together_and_reject_duplicate_coordinates() {
        let path = path("chunk-batch");
        {
            let mut store = EmberForge7Store::open(&path, 16).unwrap();
            store.apply_chunk_batch(&[
                (-1, 2, b"one".to_vec()),
                (0, 0, vec![0x44; 70_000]),
                (1, -2, b"three".to_vec()),
            ], &[]).unwrap();
            assert_eq!(store.get_chunk(1, -1, 2).unwrap(), Some(b"one".to_vec()));
            assert_eq!(store.get_chunk(1, 0, 0).unwrap(), Some(vec![0x44; 70_000]));
            let before = store.stats();
            assert!(store.apply_chunk_batch(&[
                (4, 5, b"a".to_vec()), (4, 5, b"b".to_vec()),
            ], &[]).is_err());
            assert_eq!(store.stats(), before);
            store.apply_chunk_batch(&[(0, 0, b"replacement".to_vec())], &[(-1, 2)]).unwrap();
            assert_eq!(store.get_chunk(1, -1, 2).unwrap(), None);
            assert_eq!(store.get_chunk(1, 0, 0).unwrap(), Some(b"replacement".to_vec()));
        }
        {
            let mut reopened = EmberForge7Store::open(&path, 16).unwrap();
            assert_eq!(reopened.get_chunk(1, -1, 2).unwrap(), None);
            assert_eq!(reopened.get_chunk(1, 0, 0).unwrap(), Some(b"replacement".to_vec()));
            assert_eq!(reopened.get_chunk(1, 1, -2).unwrap(), Some(b"three".to_vec()));
        }
        cleanup(&path);
    }

    #[test]
    fn empty_payload_and_namespace_validation() {
        let path = path("empty");
        let mut store = EmberForge7Store::open(&path, 8).unwrap();
        store.put_record(1, 1, &[]).unwrap();
        assert_eq!(store.get_record(1, 1).unwrap(), Some(Vec::new()));
        assert!(store.put_record(0, 1, b"x").is_err());
        assert!(store.put_record(1, 0, b"x").is_err());
        assert!(store.put_record(1, 2, &vec![0; MAX_RECORD_BYTES + 1]).is_err());
        drop(store);
        cleanup(&path);
    }
}
