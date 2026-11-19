//! Variable-length records stored across checksummed EPG1 pages.

use super::{extent::ExtentFile, page_tree};
use crate::{DbError, PageCacheStats};
use fs2::FileExt;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;
const RECORD_PAGE_MAGIC: &[u8; 4] = b"RPG1";
const RECORD_DESCRIPTOR_MAGIC: &[u8; 4] = b"RDX1";
const RECORD_PAGE_HEADER_BYTES: usize = 40;
const RECORD_CHUNK_BYTES: usize = super::extent::page_payload_size() - RECORD_PAGE_HEADER_BYTES;
const DESCRIPTOR_BYTES: usize = 32;
const MAX_RECORD_PAGES: usize = (MAX_RECORD_BYTES + RECORD_CHUNK_BYTES - 1) / RECORD_CHUNK_BYTES;

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

    /// Inserts or atomically replaces a record, including records larger than one page.
    ///
    /// Record pages are synced before the descriptor is published through the
    /// page-tree redo WAL. A crash before publication can leak unreachable pages,
    /// but cannot expose a partial record.
    pub fn put(&mut self, record_id: u64, bytes: &[u8]) -> Result<(), DbError> {
        Self::validate_record_id(record_id)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(DbError::Constraint(format!(
                "record exceeds the {MAX_RECORD_BYTES}-byte limit"
            )));
        }

        let page_count = bytes.len().max(1).div_ceil(RECORD_CHUNK_BYTES);
        if page_count > MAX_RECORD_PAGES {
            return Err(DbError::Constraint("record requires too many pages".into()));
        }
        let checksum = super::super::checksum(bytes);
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
        page_tree::insert_or_replace(
            &mut self.store,
            &self.wal_path,
            &Self::record_key(record_id),
            &descriptor.encode(),
        )
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

        if output.len() != total_len || super::super::checksum(&output) != descriptor.checksum {
            return Err(DbError::Storage("record length or whole-record checksum mismatch".into()));
        }
        Ok(Some(output))
    }

    /// Removes a record reference. Its old data pages remain allocated until vacuum.
    pub fn delete(&mut self, record_id: u64) -> Result<bool, DbError> {
        Self::validate_record_id(record_id)?;
        let key = Self::record_key(record_id);
        if page_tree::get(&mut self.store, &key)?.is_none() {
            return Ok(false);
        }
        let removed = page_tree::remove_many(&mut self.store, &self.wal_path, &[key])?;
        Ok(removed != 0)
    }

    /// Checks whether a record descriptor exists.
    pub fn contains(&mut self, record_id: u64) -> Result<bool, DbError> {
        Self::validate_record_id(record_id)?;
        Ok(page_tree::get(&mut self.store, &Self::record_key(record_id))?.is_some())
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
    use super::{RecordStore, MAX_RECORD_BYTES};
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
