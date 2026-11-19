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
const KEY_RECORD: u8 = 0x51;
const KEY_PLAYER_UUID: u8 = 0x52;
const FORMAT_VERSION: u8 = 1;

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

    /// Stores a player record and its UUID lookup entry in one B+ tree root
    /// publication. A UUID already assigned to another record is rejected.
    pub fn put_player(
        &mut self,
        namespace_id: u64,
        row_id: u64,
        uuid: [u8; 16],
        payload: &[u8],
    ) -> Result<(), DbError> {
        let row_key = record_key(namespace_id, row_id)?;
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        if let Some(existing) = page_tree::get(&mut self.pages, &lookup_key)? {
            if existing.len() != 8 {
                return Err(DbError::Storage("malformed EF7 UUID index value".into()));
            }
            let existing_id = u64::from_be_bytes(existing[..8].try_into().unwrap());
            if existing_id != row_id {
                return Err(DbError::Constraint("player UUID is already assigned to another record".into()));
            }
        }
        let old = self.pages_descriptor(&row_key)?;
        let descriptor = self.write_payload(payload)?;
        page_tree::apply_mutations(
            &mut self.pages,
            &self.wal_path,
            &[],
            &[(row_key, descriptor.encode()?), (lookup_key, row_id.to_be_bytes().to_vec())],
        )?;
        if let Some(previous) = old {
            if previous.start_page != descriptor.start_page {
                self.pages.release_extent(previous.start_page, previous.page_count as u64)?;
            }
        }
        Ok(())
    }

    /// Resolves a Minecraft player UUID to its row ID and verifies the record.
    pub fn get_player(&mut self, namespace_id: u64, uuid: [u8; 16]) -> Result<Option<(u64, Vec<u8>)>, DbError> {
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? else { return Ok(None); };
        if value.len() != 8 {
            return Err(DbError::Storage("malformed EF7 UUID index value".into()));
        }
        let row_id = u64::from_be_bytes(value[..8].try_into().unwrap());
        if row_id == 0 {
            return Err(DbError::Storage("EF7 UUID index contains reserved row ID zero".into()));
        }
        let Some(payload) = self.get_record(namespace_id, row_id)? else {
            return Err(DbError::Storage("EF7 UUID index points to a missing player record".into()));
        };
        Ok(Some((row_id, payload)))
    }

    /// Deletes a player row and UUID mapping in one root publication.
    pub fn delete_player(&mut self, namespace_id: u64, row_id: u64, uuid: [u8; 16]) -> Result<bool, DbError> {
        let row_key = record_key(namespace_id, row_id)?;
        let lookup_key = uuid_key(namespace_id, &uuid)?;
        let Some(descriptor) = self.pages_descriptor(&row_key)? else { return Ok(false); };
        let lookup = page_tree::get(&mut self.pages, &lookup_key)?;
        if let Some(value) = &lookup {
            if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != row_id {
                return Err(DbError::Constraint("UUID mapping does not belong to the requested player row".into()));
            }
        }
        let removals = if lookup.is_some() { vec![row_key, lookup_key] } else { vec![row_key] };
        let (removed, _) = page_tree::apply_mutations(&mut self.pages, &self.wal_path, &removals, &[])?;
        if removed != removals.len() {
            return Err(DbError::Storage("EF7 player deletion did not remove the expected keys".into()));
        }
        self.pages.release_extent(descriptor.start_page, descriptor.page_count as u64)?;
        Ok(true)
    }


    /// Applies player upserts and removals through one CoW-tree commit.
    /// New payload pages are durable before root publication; old extents are
    /// released only after the new root is durable.
    pub fn apply_player_batch(
        &mut self,
        upserts: &[(u64, u64, [u8; 16], Vec<u8>)],
        removals: &[(u64, u64, [u8; 16])],
    ) -> Result<(usize, usize), DbError> {
        if upserts.is_empty() && removals.is_empty() {
            return Ok((0, 0));
        }
        const MAX_BATCH_PLAYERS: usize = 2_500;
        const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
        if upserts.len().saturating_add(removals.len()) > MAX_BATCH_PLAYERS {
            return Err(DbError::Constraint(format!("EF7 player batch exceeds {MAX_BATCH_PLAYERS} operations")));
        }
        let mut upsert_rows = BTreeSet::new();
        let mut upsert_uuids = BTreeSet::new();
        let mut total_bytes = 0usize;
        for (namespace_id, row_id, uuid, payload) in upserts {
            let row_key = record_key(*namespace_id, *row_id)?;
            let lookup_key = uuid_key(*namespace_id, uuid)?;
            if !upsert_rows.insert(row_key) || !upsert_uuids.insert(lookup_key) {
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
        let mut removal_keys = Vec::with_capacity(removals.len() * 2);
        let mut removed_extents = Vec::with_capacity(removals.len());
        for (namespace_id, row_id, uuid) in removals {
            let row_key = record_key(*namespace_id, *row_id)?;
            let lookup_key = uuid_key(*namespace_id, uuid)?;
            if !removal_rows.insert(row_key.clone()) || !removal_uuids.insert(lookup_key.clone())
                || upsert_rows.contains(&row_key) || upsert_uuids.contains(&lookup_key)
            {
                return Err(DbError::Constraint("EF7 batch contains duplicate or overlapping player operations".into()));
            }
            if let Some(descriptor) = self.pages_descriptor(&row_key)? {
                removed_extents.push(descriptor);
                removal_keys.push(row_key);
                if let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? {
                    if value.len() != 8 || u64::from_be_bytes(value[..8].try_into().unwrap()) != *row_id {
                        return Err(DbError::Constraint("EF7 UUID mapping belongs to a different player row".into()));
                    }
                    removal_keys.push(lookup_key);
                }
            }
        }
        let mut upsert_entries = Vec::with_capacity(upserts.len() * 2);
        let mut replaced_extents = Vec::with_capacity(upserts.len());
        for (namespace_id, row_id, uuid, payload) in upserts {
            let row_key = record_key(*namespace_id, *row_id)?;
            let lookup_key = uuid_key(*namespace_id, uuid)?;
            if let Some(value) = page_tree::get(&mut self.pages, &lookup_key)? {
                if value.len() != 8 {
                    return Err(DbError::Storage("malformed EF7 UUID index value".into()));
                }
                if u64::from_be_bytes(value[..8].try_into().unwrap()) != *row_id {
                    return Err(DbError::Constraint("player UUID is already assigned to another record".into()));
                }
            }
            let previous = self.pages_descriptor(&row_key)?;
            let next = self.write_payload(payload)?;
            upsert_entries.push((row_key, next.encode()?));
            upsert_entries.push((lookup_key, row_id.to_be_bytes().to_vec()));
            replaced_extents.push((previous, next));
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
            page.extend_from_slice(&checksum.to_le_bytes());
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
            assert_eq!(store.apply_player_batch(&upserts, &[]).unwrap(), (0, 4));
            assert_eq!(store.get_player(7, uuid_a).unwrap(), Some((101, b"player-a".to_vec())));
            assert_eq!(store.get_player(7, uuid_b).unwrap(), Some((102, vec![0x5a; 90_000])));
            let replacement = vec![(7, 102, uuid_b, b"updated-player-b".to_vec())];
            let removals = vec![(7, 101, uuid_a)];
            assert_eq!(store.apply_player_batch(&replacement, &removals).unwrap(), (2, 2));
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
