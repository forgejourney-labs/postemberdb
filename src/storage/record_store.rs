//! Variable-length records stored across checksummed EPG1 pages.

use super::{extent::ExtentFile, page_tree};
use crate::{DbError, PageCacheStats};
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
const RECORD_PAGE_MAGIC: &[u8; 4] = b"RPG1";
const RECORD_DESCRIPTOR_MAGIC: &[u8; 4] = b"RDX1";
const RECORD_PAGE_HEADER_BYTES: usize = 40;
const RECORD_CHUNK_BYTES: usize = super::extent::page_payload_size() - RECORD_PAGE_HEADER_BYTES;
const DESCRIPTOR_BYTES: usize = 32;
const MAX_RECORD_PAGES: usize = (MAX_RECORD_BYTES + RECORD_CHUNK_BYTES - 1) / RECORD_CHUNK_BYTES;
const MAX_BATCH_RECORDS: usize = 5_000;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[derive(Debug, Clone, Copy)]
struct RecordDescriptor {
    first_page_id: u64,
    page_count: u32,
    total_len: u64,
    checksum: u64,
}

impl RecordDescriptor {
    fn encode(self) -> [u8; DESCRIPTOR_BYTES] {
        let mut bytes = [0u8; DESCRIPTOR_BYTES];
        bytes[0..4].copy_from_slice(RECORD_DESCRIPTOR_MAGIC);
        bytes[4..12].copy_from_slice(&self.first_page_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.page_count.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.total_len.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != DESCRIPTOR_BYTES || &bytes[0..4] != RECORD_DESCRIPTOR_MAGIC {
            return Err(DbError::Storage("invalid record descriptor".into()));
        }
        let descriptor = Self {
            first_page_id: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            page_count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            total_len: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            checksum: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        };
        let total_len = usize::try_from(descriptor.total_len)
            .map_err(|_| DbError::Storage("record length does not fit this platform".into()))?;
        if total_len > MAX_RECORD_BYTES {
            return Err(DbError::Storage("record descriptor exceeds the maximum record size".into()));
        }
        let expected_pages = total_len.max(1).div_ceil(RECORD_CHUNK_BYTES);
        if descriptor.page_count == 0
            || descriptor.page_count as usize != expected_pages
            || descriptor.page_count as usize > MAX_RECORD_PAGES
            || descriptor.first_page_id.checked_add(u64::from(descriptor.page_count)).is_none()
        {
            return Err(DbError::Storage("invalid record page extent in descriptor".into()));
        }
        Ok(descriptor)
    }
}

/// Persistent variable-length record storage built on ExtentFile and EmberTree.
///
/// Record identifiers are caller-supplied, non-zero u64 values. Record bytes
/// are stored over one or more EPG1 pages; an EmberTree index maps each ID to its
/// page extent and whole-record checksum. Replaced/deleted record extents are
/// not reclaimed yet; reachability-aware vacuum is a separate operation.
pub struct RecordStore {
    store: ExtentFile,
    wal_path: PathBuf,
    _lock_file: File,
}

impl RecordStore {
    /// Opens (or creates) a record store with the default 128-page LRU cache.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_cache_capacity(path, super::cache::DEFAULT_PAGE_CACHE_CAPACITY)
    }

    /// Opens a record store with a configurable decoded-page cache capacity.
    pub fn open_with_cache_capacity(
        path: impl AsRef<Path>,
        cache_capacity_pages: usize,
    ) -> Result<Self, DbError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(storage_error)?;
            }
        }
        let lock_path = with_suffix(path, ".lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(storage_error)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("record store is already open: {error}")))?;

        let pages_path = with_suffix(path, ".pages");
        let wal_path = with_suffix(path, ".pages.wal");
        let mut store = ExtentFile::open_with_cache_capacity(&pages_path, cache_capacity_pages)?;
        page_tree::recover(&mut store, &wal_path)?;
        page_tree::validate_tree(&mut store)?;
        Ok(Self { store, wal_path, _lock_file: lock_file })
    }

    fn validate_record_id(record_id: u64) -> Result<(), DbError> {
        if record_id == 0 {
            return Err(DbError::Constraint("record ID zero is reserved".into()));
        }
        Ok(())
    }

    fn record_key(record_id: u64) -> Vec<u8> {
        record_id.to_be_bytes().to_vec()
    }

    /// Inserts or atomically replaces one record, including multi-page records.
    pub fn put(&mut self, record_id: u64, bytes: &[u8]) -> Result<(), DbError> {
        self.put_many([(record_id, bytes.to_vec())]).map(|_| ())
    }

    /// Writes up to 5,000 records with one B+ Tree redo record and root publication.
    ///
    /// Duplicate IDs use last-write-wins semantics. All IDs, sizes, and the
    /// aggregate batch budget are checked before any page is allocated. Every
    /// data extent is synced before the descriptor batch is published; a crash
    /// before publication can leak unreachable extents but cannot expose a
    /// partial batch.
    pub fn put_many<I>(&mut self, records: I) -> Result<usize, DbError>
    where
        I: IntoIterator<Item = (u64, Vec<u8>)>,
    {
        let batch = records.into_iter().take(MAX_BATCH_RECORDS + 1).collect::<Vec<_>>();
        if batch.len() > MAX_BATCH_RECORDS {
            return Err(DbError::Constraint(format!(
                "record batch exceeds the maximum of {MAX_BATCH_RECORDS} records"
            )));
        }
        if batch.is_empty() {
            return Ok(0);
        }

        let mut unique = BTreeMap::<u64, Vec<u8>>::new();
        for (record_id, bytes) in batch {
            Self::validate_record_id(record_id)?;
            if bytes.len() > MAX_RECORD_BYTES {
                return Err(DbError::Constraint(format!(
                    "record exceeds the {MAX_RECORD_BYTES}-byte limit"
                )));
            }
            unique.insert(record_id, bytes);
        }

        let total_batch_bytes = unique.values().try_fold(0usize, |total, bytes| {
            total.checked_add(bytes.len())
                .ok_or_else(|| DbError::Constraint("record batch byte count overflow".into()))
        })?;
        if total_batch_bytes > MAX_BATCH_BYTES {
            return Err(DbError::Constraint(format!(
                "record batch exceeds the {MAX_BATCH_BYTES}-byte aggregate limit"
            )));
        }

        let mut descriptors = Vec::with_capacity(unique.len());
        for (record_id, bytes) in &unique {
            let page_count = bytes.len().max(1).div_ceil(RECORD_CHUNK_BYTES);
            if page_count > MAX_RECORD_PAGES {
                return Err(DbError::Constraint("record requires too many pages".into()));
            }
            let checksum = super::checksum(bytes);
            let first_page_id = self.store.allocate_extent(page_count as u64)?;
            for page_index in 0..page_count {
                let start = page_index * RECORD_CHUNK_BYTES;
                let end = bytes.len().min(start + RECORD_CHUNK_BYTES);
                let chunk = &bytes[start..end];

                let mut payload = Vec::with_capacity(RECORD_PAGE_HEADER_BYTES + chunk.len());
                payload.extend_from_slice(RECORD_PAGE_MAGIC);
                payload.extend_from_slice(&record_id.to_le_bytes());
                payload.extend_from_slice(&(page_index as u32).to_le_bytes());
                payload.extend_from_slice(&(page_count as u32).to_le_bytes());
                payload.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                payload.extend_from_slice(&checksum.to_le_bytes());
                payload.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
                payload.extend_from_slice(chunk);
                self.store.write_page(first_page_id + page_index as u64, &payload)?;
            }

            let descriptor = RecordDescriptor {
                first_page_id,
                page_count: page_count as u32,
                total_len: bytes.len() as u64,
                checksum,
            };
            descriptors.push((Self::record_key(*record_id), descriptor.encode().to_vec()));
        }

        page_tree::insert_many(&mut self.store, &self.wal_path, &descriptors)?;
        Ok(unique.len())
    }

    /// Reads and validates a complete record. Missing IDs return None.
    pub fn get(&mut self, record_id: u64) -> Result<Option<Vec<u8>>, DbError> {
        Self::validate_record_id(record_id)?;
        let Some(encoded_descriptor) = page_tree::get(&mut self.store, &Self::record_key(record_id))? else {
            return Ok(None);
        };
        let descriptor = RecordDescriptor::decode(&encoded_descriptor)?;
        let total_len = usize::try_from(descriptor.total_len)
            .map_err(|_| DbError::Storage("record length does not fit this platform".into()))?;
        let mut output = Vec::with_capacity(total_len);

        for page_index in 0..descriptor.page_count {
            let page_id = descriptor.first_page_id.checked_add(u64::from(page_index))
                .ok_or_else(|| DbError::Storage("record page ID overflow".into()))?;
            let payload = self.store.read_page(page_id)?;
            if payload.len() < RECORD_PAGE_HEADER_BYTES
                || &payload[0..4] != RECORD_PAGE_MAGIC
            {
                return Err(DbError::Storage(format!("invalid record page {page_id}")));
            }

            let stored_record_id = u64::from_le_bytes(payload[4..12].try_into().unwrap());
            let stored_index = u32::from_le_bytes(payload[12..16].try_into().unwrap());
            let stored_count = u32::from_le_bytes(payload[16..20].try_into().unwrap());
            let stored_total_len = u64::from_le_bytes(payload[20..28].try_into().unwrap());
            let stored_checksum = u64::from_le_bytes(payload[28..36].try_into().unwrap());
            let chunk_len = u32::from_le_bytes(payload[36..40].try_into().unwrap()) as usize;

            if stored_record_id != record_id
                || stored_index != page_index
                || stored_count != descriptor.page_count
                || stored_total_len != descriptor.total_len
                || stored_checksum != descriptor.checksum
                || chunk_len != payload.len() - RECORD_PAGE_HEADER_BYTES
            {
                return Err(DbError::Storage(format!("record page metadata mismatch at page {page_id}")));
            }
            if output.len().checked_add(chunk_len).is_none_or(|length| length > total_len) {
                return Err(DbError::Storage("record chunks exceed the declared record length".into()));
            }
            output.extend_from_slice(&payload[RECORD_PAGE_HEADER_BYTES..]);
        }

        if output.len() != total_len || super::checksum(&output) != descriptor.checksum {
            return Err(DbError::Storage("record length or whole-record checksum mismatch".into()));
        }
        Ok(Some(output))
    }

    /// Removes one record reference. Its old data pages remain allocated until vacuum.
    pub fn delete(&mut self, record_id: u64) -> Result<bool, DbError> {
        Ok(self.delete_many([record_id])? != 0)
    }

    /// Removes up to 5,000 record descriptors through one CoW root publication.
    /// Duplicate IDs are coalesced; missing IDs are ignored.
    pub fn delete_many<I>(&mut self, record_ids: I) -> Result<usize, DbError>
    where
        I: IntoIterator<Item = u64>,
    {
        let batch = record_ids.into_iter().take(MAX_BATCH_RECORDS + 1).collect::<Vec<_>>();
        if batch.len() > MAX_BATCH_RECORDS {
            return Err(DbError::Constraint(format!(
                "record delete batch exceeds the maximum of {MAX_BATCH_RECORDS} IDs"
            )));
        }
        let mut unique = BTreeSet::new();
        for record_id in batch {
            Self::validate_record_id(record_id)?;
            unique.insert(record_id);
        }
        let keys = unique.into_iter().map(Self::record_key).collect::<Vec<_>>();
        page_tree::remove_many(&mut self.store, &self.wal_path, &keys)
    }

    /// Checks whether a record descriptor exists.
    pub fn contains(&mut self, record_id: u64) -> Result<bool, DbError> {
        Self::validate_record_id(record_id)?;
        Ok(page_tree::get(&mut self.store, &Self::record_key(record_id))?.is_some())
    }

    /// Returns record IDs in ascending order without loading record payloads.
    ///
    /// The scan is inclusive of the first ID after `after`, and returns at
    /// most 5,000 IDs. Callers can pass the last returned ID to the next call
    /// to paginate through a large store while keeping memory bounded.
    pub fn scan_ids(&mut self, after: Option<u64>, limit: usize) -> Result<Vec<u64>, DbError> {
        if limit > MAX_BATCH_RECORDS {
            return Err(DbError::Constraint(format!(
                "record scan limit cannot exceed {MAX_BATCH_RECORDS} IDs"
            )));
        }
        if limit == 0 || after == Some(u64::MAX) {
            return Ok(Vec::new());
        }

        let first_id = match after {
            Some(previous) => previous.checked_add(1)
                .ok_or_else(|| DbError::Constraint("record scan cursor overflow".into()))?,
            None => 1,
        };
        let lower = first_id.to_be_bytes();
        let upper = u64::MAX.to_be_bytes();
        let entries = page_tree::scan_range_limited(
            &mut self.store,
            &lower,
            &upper,
            Some(limit),
        )?;

        let mut ids = Vec::with_capacity(entries.len());
        for (key, _) in entries {
            if key.len() != 8 {
                return Err(DbError::Storage("record index contains an invalid ID key".into()));
            }
            let record_id = u64::from_be_bytes(key.as_slice().try_into().unwrap());
            if record_id == 0 || ids.last().is_some_and(|previous| *previous >= record_id) {
                return Err(DbError::Storage("record index IDs are zero or not strictly ordered".into()));
            }
            ids.push(record_id);
        }
        Ok(ids)
    }

    /// Returns the underlying extent-page cache metrics.
    pub fn page_cache_stats(&self) -> PageCacheStats {
        self.store.page_cache_stats()
    }

    /// Adjusts the decoded-page cache capacity for this handle.
    pub fn set_page_cache_capacity(&mut self, capacity_pages: usize) -> Result<(), DbError> {
        self.store.set_page_cache_capacity(capacity_pages)
    }
}

fn storage_error(error: std::io::Error) -> DbError {
    DbError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{page_tree, RecordStore, MAX_RECORD_BYTES};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "postemberdb-record-store-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    fn remove_store_files(path: &PathBuf) {
        for suffix in [".lock", ".pages", ".pages.wal"] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = fs::remove_file(PathBuf::from(candidate));
        }
    }

    #[test]
    fn variable_length_records_round_trip_across_pages_and_reopen() {
        let path = temp_path("multi-page");
        let large = (0..150_000usize).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        {
            let mut store = RecordStore::open(&path).unwrap();
            store.put(7, &large).unwrap();
            assert_eq!(store.get(7).unwrap(), Some(large.clone()));
            assert!(store.contains(7).unwrap());

            // Replaced immutable pages remain allocated until a future vacuum.
            store.put(7, b"replacement").unwrap();
            assert_eq!(store.get(7).unwrap(), Some(b"replacement".to_vec()));
            store.put(8, b"").unwrap();
            assert_eq!(store.get(8).unwrap(), Some(Vec::new()));
        }

        let mut reopened = RecordStore::open(&path).unwrap();
        assert_eq!(reopened.get(7).unwrap(), Some(b"replacement".to_vec()));
        assert_eq!(reopened.get(8).unwrap(), Some(Vec::new()));
        assert!(reopened.delete(7).unwrap());
        assert!(!reopened.delete(7).unwrap());
        assert_eq!(reopened.get(7).unwrap(), None);
        drop(reopened);
        remove_store_files(&path);
    }

    #[test]
    fn record_store_batches_multiple_ids_with_last_write_wins() {
        let path = temp_path("batch");
        let mut store = RecordStore::open(&path).unwrap();
        assert_eq!(store.put_many(vec![
            (10, b"old".to_vec()),
            (11, vec![0xA5; 25_000]),
            (10, b"new".to_vec()),
            (12, Vec::new()),
        ]).unwrap(), 3);

        assert_eq!(store.get(10).unwrap(), Some(b"new".to_vec()));
        assert_eq!(store.get(11).unwrap(), Some(vec![0xA5; 25_000]));
        assert_eq!(store.get(12).unwrap(), Some(Vec::new()));
        assert_eq!(store.delete_many([10, 10, 404, 11]).unwrap(), 2);
        assert_eq!(store.get(10).unwrap(), None);
        assert_eq!(store.get(11).unwrap(), None);
        assert_eq!(store.get(12).unwrap(), Some(Vec::new()));
        drop(store);
        remove_store_files(&path);
    }

    #[test]
    fn record_store_validates_batch_before_reserving_any_pages() {
        let path = temp_path("batch-validation");
        let mut store = RecordStore::open(&path).unwrap();
        let initial_pages = store.store.stats().allocated_pages;
        assert!(store.put_many(vec![
            (1, b"valid".to_vec()),
            (0, b"invalid id".to_vec()),
        ]).is_err());
        assert_eq!(store.store.stats().allocated_pages, initial_pages);
        assert_eq!(store.get(1).unwrap(), None);
        drop(store);
        remove_store_files(&path);
    }

    #[test]
    fn record_store_rejects_reserved_ids_and_oversized_records() {
        let path = temp_path("bounds");
        let mut store = RecordStore::open(&path).unwrap();
        assert!(store.put(0, b"invalid").is_err());
        assert!(store.get(0).is_err());
        assert!(store.delete(0).is_err());
        assert!(store.put(1, &vec![0u8; MAX_RECORD_BYTES + 1]).is_err());
        drop(store);
        remove_store_files(&path);
    }

    #[test]
    fn record_store_scans_ids_in_bounded_stable_pages() {
        let path = temp_path("scan-ids");
        {
            let mut store = RecordStore::open(&path).unwrap();
            store.put_many(vec![
                (40, b"four".to_vec()),
                (10, b"one".to_vec()),
                (30, b"three".to_vec()),
                (20, b"two".to_vec()),
            ]).unwrap();

            assert_eq!(store.scan_ids(None, 2).unwrap(), vec![10, 20]);
            assert_eq!(store.scan_ids(Some(20), 2).unwrap(), vec![30, 40]);
            assert!(store.scan_ids(Some(40), 2).unwrap().is_empty());
            assert!(store.scan_ids(None, 0).unwrap().is_empty());
            assert!(store.scan_ids(None, 5_001).is_err());

            store.delete(20).unwrap();
            assert_eq!(store.scan_ids(Some(10), 10).unwrap(), vec![30, 40]);
        }

        let mut reopened = RecordStore::open(&path).unwrap();
        assert_eq!(reopened.scan_ids(None, 10).unwrap(), vec![10, 30, 40]);
        assert_eq!(reopened.get(30).unwrap(), Some(b"three".to_vec()));
        drop(reopened);
        remove_store_files(&path);
    }

    #[test]
    fn record_store_rejects_corrupt_record_page_metadata() {
        let path = temp_path("corruption");
        let mut store = RecordStore::open(&path).unwrap();
        store.put(99, b"record-data").unwrap();
        let encoded = page_tree::get(&mut store.store, &RecordStore::record_key(99)).unwrap().unwrap();
        let descriptor = super::RecordDescriptor::decode(&encoded).unwrap();
        let page_id = descriptor.first_page_id;
        let mut payload = store.store.read_page(page_id).unwrap();
        payload[4] ^= 0x01;
        store.store.write_page(page_id, &payload).unwrap();
        assert!(store.get(99).unwrap_err().to_string().contains("metadata mismatch"));
        drop(store);
        remove_store_files(&path);
    }
}
