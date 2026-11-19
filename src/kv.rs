//! Standalone persistent key/value storage and a typed, registry-like hierarchy.

use crate::storage::{cache::DEFAULT_PAGE_CACHE_CAPACITY, extent::ExtentFile, page_tree, participant::{self, PreparedBatch}};
use crate::{DbError, PageCacheStats};
use fs2::FileExt;
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const MAX_KEY_BYTES: usize = 1024;

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

/// A persistent byte-oriented key/value store backed by PostemberDB's native
/// copy-on-write B+ Tree. Each batch is one redo-WAL transaction and one root
/// publication. Keys are 1..=1024 bytes; values must fit alongside their keys in one leaf entry.
pub struct KeyValueStore {
    store: ExtentFile,
    wal_path: PathBuf,
    _lock_file: File,
    prepared_transactions: BTreeSet<u64>,
}

impl KeyValueStore {
    /// Opens a KV store with a bounded 128-page cache by default.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_cache_capacity(path, DEFAULT_PAGE_CACHE_CAPACITY)
    }

    /// Opens a KV store with a caller-selected decoded-page cache capacity.
    /// Use zero to disable caching; capacities above 65,536 pages are rejected.
    pub fn open_with_cache_capacity(
        path: impl AsRef<Path>,
        cache_capacity_pages: usize,
    ) -> Result<Self, DbError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|error| DbError::Storage(error.to_string()))?;
            }
        }
        let lock_path = with_suffix(path, ".lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|error| DbError::Storage(error.to_string()))?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("key/value store is already open: {error}")))?;

        let pages_path = with_suffix(path, ".pages");
        let wal_path = with_suffix(path, ".pages.wal");
        let mut store = ExtentFile::open_with_cache_capacity(&pages_path, cache_capacity_pages)?;
        page_tree::recover(&mut store, &wal_path)?;
        page_tree::validate_tree(&mut store)?;
        let prepared_transactions = participant::list_prepared(&wal_path)?;
        Ok(Self { store, wal_path, _lock_file: lock_file, prepared_transactions })
    }

    /// Durably stages a bounded mutation batch for a coordinator transaction.
    /// The batch is not visible until the coordinator has recorded COMMIT and
    /// calls commit_prepared; the prepared marker survives process restart.
    pub fn prepare_batch(
        &mut self,
        transaction_id: u64,
        removals: &[Vec<u8>],
        upserts: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), DbError> {
        if !self.prepared_transactions.is_empty() && !self.prepared_transactions.contains(&transaction_id) {
            return Err(DbError::Storage("KV store has another unresolved prepared transaction".into()));
        }
        for key in removals { Self::validate_key(key)?; }
        for (key, _) in upserts { Self::validate_key(key)?; }
        let batch = PreparedBatch { removals: removals.to_vec(), upserts: upserts.to_vec() };
        participant::write_prepared(&self.wal_path, transaction_id, &batch)?;
        self.prepared_transactions.insert(transaction_id);
        Ok(())
    }

    /// Applies a previously prepared batch. Safe to retry after a crash because
    /// the underlying operations are deterministic upserts/removals.
    pub fn commit_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        let batch = participant::read_prepared(&self.wal_path, transaction_id)?;
        page_tree::apply_mutations(&mut self.store, &self.wal_path, &batch.removals, &batch.upserts)?;
        Ok(())
    }

    /// Removes the prepared marker after the coordinator has durably recorded
    /// this participant's APPLIED acknowledgement.
    pub fn finalize_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        participant::remove_prepared(&self.wal_path, transaction_id)?;
        self.prepared_transactions.remove(&transaction_id);
        Ok(())
    }

    /// Discards a prepared batch after the coordinator has durably recorded ABORT.
    pub fn rollback_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        participant::remove_prepared(&self.wal_path, transaction_id)?;
        self.prepared_transactions.remove(&transaction_id);
        Ok(())
    }

    pub fn pending_prepared_transactions(&self) -> Vec<u64> {
        self.prepared_transactions.iter().copied().collect()
    }

    fn ensure_no_prepared_transactions(&self) -> Result<(), DbError> {
        if self.prepared_transactions.is_empty() {
            Ok(())
        } else {
            Err(DbError::Storage("KV writes are blocked while a coordinator transaction is unresolved".into()))
        }
    }

    /// Returns the cache occupancy and cumulative hit/miss/eviction counters.
    pub fn page_cache_stats(&self) -> PageCacheStats {
        self.store.page_cache_stats()
    }

    /// Changes this handle's page-cache capacity; shrinking evicts least-recently-used pages.
    pub fn set_page_cache_capacity(&mut self, capacity_pages: usize) -> Result<(), DbError> {
        self.store.set_page_cache_capacity(capacity_pages)
    }

    fn validate_key(key: &[u8]) -> Result<(), DbError> {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("key must contain 1..=1024 bytes".into()));
        }
        Ok(())
    }

    /// Returns the value for an exact byte key.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        Self::validate_key(key)?;
        page_tree::get(&mut self.store, key)
    }

    /// Inserts or replaces one key atomically.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        self.put_many(&[(key.to_vec(), value.to_vec())])
    }

    /// Applies multiple inserts/replacements in one CoW publication.
    /// If a key occurs more than once, the last supplied value wins.
    pub fn put_many(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
        self.ensure_no_prepared_transactions()?;
        for (key, _) in entries {
            Self::validate_key(key)?;
        }
        page_tree::insert_many(&mut self.store, &self.wal_path, entries)
    }

    /// Removes one key. Returns false when it did not exist.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, DbError> {
        self.ensure_no_prepared_transactions()?;
        Self::validate_key(key)?;
        let (removed, _) = page_tree::apply_mutations(
            &mut self.store,
            &self.wal_path,
            &[key.to_vec()],
            &[],
        )?;
        Ok(removed != 0)
    }

    /// Applies removals and upserts as one atomic page-tree transaction.
    pub fn apply_batch(
        &mut self,
        removals: &[Vec<u8>],
        upserts: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(usize, usize), DbError> {
        self.ensure_no_prepared_transactions()?;
        for key in removals {
            Self::validate_key(key)?;
        }
        for (key, _) in upserts {
            Self::validate_key(key)?;
        }
        page_tree::apply_mutations(&mut self.store, &self.wal_path, removals, upserts)
    }

    /// Returns all key/value pairs whose keys begin with the supplied prefix.
    /// Results are in bytewise key order.
    pub fn scan_prefix(&mut self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
        if prefix.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("prefix cannot exceed 1024 bytes".into()));
        }
        let lower = if prefix.is_empty() { vec![0] } else { prefix.to_vec() };
        let mut upper = prefix.to_vec();
        upper.resize(MAX_KEY_BYTES, u8::MAX);
        page_tree::scan_range_limited(&mut self.store, &lower, &upper, limit)
    }

    /// Removes up to `max_items` keys under a prefix in one atomic CoW batch.
    ///
    /// The batch is bounded to 5,000 keys. Call repeatedly until this returns
    /// zero to drain a larger prefix without materializing the entire range.
    /// Each call is atomic, but a sequence of calls is not one global transaction.
    pub fn delete_prefix_bounded(&mut self, prefix: &[u8], max_items: usize) -> Result<usize, DbError> {
        self.ensure_no_prepared_transactions()?;
        if prefix.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("prefix cannot exceed 1024 bytes".into()));
        }
        const MAX_DELETE_BATCH: usize = 5_000;
        if max_items > MAX_DELETE_BATCH {
            return Err(DbError::Constraint(format!(
                "bounded prefix deletion cannot exceed {MAX_DELETE_BATCH} keys per batch"
            )));
        }
        if max_items == 0 {
            return Ok(0);
        }

        let entries = self.scan_prefix(prefix, Some(max_items))?;
        if entries.is_empty() {
            return Ok(0);
        }
        let removals = entries.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
        let (removed, _) = page_tree::apply_mutations(&mut self.store, &self.wal_path, &removals, &[])?;
        Ok(removed)
    }

    /// Removes every key under a prefix in one CoW publication.
    ///
    /// This convenience method may materialize the complete matching key set.
    /// Use `delete_prefix_bounded` when the prefix may contain a large range.
    pub fn delete_prefix(&mut self, prefix: &[u8]) -> Result<usize, DbError> {
        self.ensure_no_prepared_transactions()?;
        let entries = self.scan_prefix(prefix, None)?;
        let removals = entries.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
        let (removed, _) = page_tree::apply_mutations(&mut self.store, &self.wal_path, &removals, &[])?;
        Ok(removed)
    }
}

/// Strongly typed values supported by the registry-like configuration tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Binary(Vec<u8>),
    Json(String),
}

impl RegistryValue {
    fn encode(&self) -> Vec<u8> {
        let mut out = vec![1];
        match self {
            Self::String(value) => { out.push(1); out.extend_from_slice(value.as_bytes()); }
            Self::Integer(value) => { out.push(2); out.extend_from_slice(&value.to_le_bytes()); }
            Self::Boolean(value) => { out.push(3); out.push(u8::from(*value)); }
            Self::Binary(value) => { out.push(4); out.extend_from_slice(value); }
            Self::Json(value) => { out.push(5); out.extend_from_slice(value.as_bytes()); }
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < 2 || bytes[0] != 1 {
            return Err(DbError::Storage("invalid registry value encoding".into()));
        }
        match bytes[1] {
            1 => Ok(Self::String(String::from_utf8(bytes[2..].to_vec())
                .map_err(|_| DbError::Storage("registry string is not valid UTF-8".into()))?)),
            2 if bytes.len() == 10 => Ok(Self::Integer(i64::from_le_bytes(bytes[2..10].try_into().unwrap()))),
            3 if bytes.len() == 3 && bytes[2] <= 1 => Ok(Self::Boolean(bytes[2] == 1)),
            4 => Ok(Self::Binary(bytes[2..].to_vec())),
            5 => Ok(Self::Json(String::from_utf8(bytes[2..].to_vec())
                .map_err(|_| DbError::Storage("registry JSON value is not valid UTF-8".into()))?)),
            _ => Err(DbError::Storage("invalid registry value type or payload length".into())),
        }
    }
}

fn normalize_path(path: &str) -> Result<String, DbError> {
    let mut components = Vec::new();
    for component in path.split(|character| character == '/' || character == '\\') {
        let component = component.trim();
        if component.is_empty() { continue; }
        if component == "." || component == ".." || component.contains('\0') {
            return Err(DbError::Constraint("registry paths cannot contain '.', '..', or NUL components".into()));
        }
        components.push(component.to_lowercase());
    }
    if components.is_empty() {
        return Err(DbError::Constraint("registry path must include a hive".into()));
    }
    Ok(components.join("/"))
}

fn key_record(path: &str) -> Vec<u8> {
    let mut key = vec![b'K'];
    key.extend_from_slice(path.as_bytes());
    key
}

fn child_key_prefix(path: &str) -> Vec<u8> {
    let mut key = key_record(path);
    key.push(b'/');
    key
}

fn value_record_prefix(path: &str) -> Vec<u8> {
    let mut key = vec![b'V'];
    key.extend_from_slice(path.as_bytes());
    key.push(0);
    key
}

/// Registry-like hierarchical configuration stored in its own persistent KV
/// tree. Paths are case-insensitive and accept either slash style.
pub struct Registry {
    store: KeyValueStore,
}

impl Registry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Ok(Self { store: KeyValueStore::open(path)? })
    }
    pub fn pending_prepared_transactions(&self) -> Vec<u64> {
        self.store.pending_prepared_transactions()
    }

    pub fn commit_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        self.store.commit_prepared(transaction_id)
    }

    pub fn finalize_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        self.store.finalize_prepared(transaction_id)
    }

    pub fn rollback_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        self.store.rollback_prepared(transaction_id)
    }

    /// Stages a registry value update for a coordinator transaction.
    pub fn prepare_set_value(
        &mut self,
        transaction_id: u64,
        path: &str,
        name: &str,
        value: RegistryValue,
    ) -> Result<(), DbError> {
        let normalized = normalize_path(path)?;
        if name.contains('\0') {
            return Err(DbError::Constraint("registry value name cannot contain NUL".into()));
        }
        let mut key = value_record_prefix(&normalized);
        key.extend_from_slice(name.to_lowercase().as_bytes());
        self.store.prepare_batch(transaction_id, &[], &[(key, value.encode())])
    }

    /// Stages a registry value deletion for a coordinator transaction.
    pub fn prepare_delete_value(
        &mut self,
        transaction_id: u64,
        path: &str,
        name: &str,
    ) -> Result<(), DbError> {
        let normalized = normalize_path(path)?;
        if name.contains('\0') {
            return Err(DbError::Constraint("registry value name cannot contain NUL".into()));
        }
        let mut key = value_record_prefix(&normalized);
        key.extend_from_slice(name.to_lowercase().as_bytes());
        self.store.prepare_batch(transaction_id, &[key], &[])
    }

    /// Creates a top-level hive, for example `SYSTEM` or `USERS`.
    pub fn create_hive(&mut self, name: &str) -> Result<(), DbError> {
        let normalized = normalize_path(name)?;
        if normalized.contains('/') {
            return Err(DbError::Constraint("hive name must be a single path component".into()));
        }
        self.store.put(&key_record(&normalized), &[1])
    }

    /// Creates a key and any missing ancestors in one atomic batch.
    pub fn create_key(&mut self, path: &str) -> Result<(), DbError> {
        let normalized = normalize_path(path)?;
        let mut current = String::new();
        let mut entries = Vec::new();
        for component in normalized.split('/') {
            if !current.is_empty() { current.push('/'); }
            current.push_str(component);
            entries.push((key_record(&current), vec![1]));
        }
        self.store.put_many(&entries)
    }

    pub fn key_exists(&mut self, path: &str) -> Result<bool, DbError> {
        let normalized = normalize_path(path)?;
        Ok(self.store.get(&key_record(&normalized))?.is_some())
    }

    /// Sets or replaces a named value under an existing key. An empty name is
    /// permitted and represents the default value, as in the Windows registry.
    pub fn set_value(&mut self, path: &str, name: &str, value: RegistryValue) -> Result<(), DbError> {
        let normalized = normalize_path(path)?;
        if name.contains('\0') {
            return Err(DbError::Constraint("registry value name cannot contain NUL".into()));
        }
        if self.store.get(&key_record(&normalized))?.is_none() {
            return Err(DbError::Constraint(format!("registry key does not exist: {path}")));
        }
        let mut key = value_record_prefix(&normalized);
        key.extend_from_slice(name.to_lowercase().as_bytes());
        self.store.put(&key, &value.encode())
    }

    pub fn get_value(&mut self, path: &str, name: &str) -> Result<Option<RegistryValue>, DbError> {
        let normalized = normalize_path(path)?;
        if name.contains('\0') {
            return Err(DbError::Constraint("registry value name cannot contain NUL".into()));
        }
        let mut key = value_record_prefix(&normalized);
        key.extend_from_slice(name.to_lowercase().as_bytes());
        self.store.get(&key)?.map(|value| RegistryValue::decode(&value)).transpose()
    }

    pub fn delete_value(&mut self, path: &str, name: &str) -> Result<bool, DbError> {
        let normalized = normalize_path(path)?;
        if name.contains('\0') {
            return Err(DbError::Constraint("registry value name cannot contain NUL".into()));
        }
        let mut key = value_record_prefix(&normalized);
        key.extend_from_slice(name.to_lowercase().as_bytes());
        self.store.delete(&key)
    }

    pub fn list_hives(&mut self) -> Result<Vec<String>, DbError> {
        let entries = self.store.scan_prefix(b"K", None)?;
        let mut names = BTreeSet::new();
        for (key, _) in entries {
            if key.len() > 1 {
                let path = String::from_utf8_lossy(&key[1..]);
                if let Some(hive) = path.split('/').next() { names.insert(hive.to_owned()); }
            }
        }
        Ok(names.into_iter().collect())
    }

    pub fn list_subkeys(&mut self, path: &str) -> Result<Vec<String>, DbError> {
        let normalized = normalize_path(path)?;
        let prefix = child_key_prefix(&normalized);
        let entries = self.store.scan_prefix(&prefix, None)?;
        let mut names = BTreeSet::new();
        for (key, _) in entries {
            let suffix = &key[prefix.len()..];
            if let Some(component) = suffix.split(|byte| *byte == b'/').next() {
                if !component.is_empty() {
                    names.insert(String::from_utf8(component.to_vec())
                        .map_err(|_| DbError::Storage("registry key name is not valid UTF-8".into()))?);
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    pub fn list_values(&mut self, path: &str) -> Result<Vec<String>, DbError> {
        let normalized = normalize_path(path)?;
        let prefix = value_record_prefix(&normalized);
        let entries = self.store.scan_prefix(&prefix, None)?;
        entries.into_iter().map(|(key, _)| {
            String::from_utf8(key[prefix.len()..].to_vec())
                .map_err(|_| DbError::Storage("registry value name is not valid UTF-8".into()))
        }).collect()
    }

    /// Deletes a key. Non-recursive deletion is rejected while it has children
    /// or values; recursive deletion removes its entire subtree atomically.
    pub fn delete_key(&mut self, path: &str, recursive: bool) -> Result<bool, DbError> {
        let normalized = normalize_path(path)?;
        let exact = key_record(&normalized);
        if self.store.get(&exact)?.is_none() { return Ok(false); }
        let children = self.list_subkeys(&normalized)?;
        let values = self.list_values(&normalized)?;
        if !recursive && (!children.is_empty() || !values.is_empty()) {
            return Err(DbError::Constraint("registry key is not empty; use recursive deletion".into()));
        }

        let mut removals = vec![exact];
        if recursive {
            let child_prefix = child_key_prefix(&normalized);
            let mut child_keys = self.store.scan_prefix(&child_prefix, None)?
                .into_iter().map(|(key, _)| key).collect::<Vec<_>>();
            removals.append(&mut child_keys);

            let value_prefix = value_record_prefix(&normalized);
            let mut value_keys = self.store.scan_prefix(&value_prefix, None)?
                .into_iter().map(|(key, _)| key).collect::<Vec<_>>();
            removals.append(&mut value_keys);

            let mut descendant_values_prefix = child_prefix;
            descendant_values_prefix[0] = b'V';
            let mut descendant_values = self.store.scan_prefix(&descendant_values_prefix, None)?
                .into_iter().map(|(key, _)| key).collect::<Vec<_>>();
            removals.append(&mut descendant_values);
        }
        self.store.apply_batch(&removals, &[])?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "postemberdb-{label}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    #[test]
    fn key_value_store_batches_prefix_scans_and_reopens() {
        let path = temp_path("kv");
        {
            let mut store = KeyValueStore::open(&path).unwrap();
            store.put_many(&[
                (b"players/a".to_vec(), b"Ada".to_vec()),
                (b"players/b".to_vec(), b"Grace".to_vec()),
                (b"config/mode".to_vec(), b"survival".to_vec()),
            ]).unwrap();
            assert_eq!(store.get(b"players/a").unwrap(), Some(b"Ada".to_vec()));
            let players = store.scan_prefix(b"players/", None).unwrap();
            assert_eq!(players.len(), 2);
            assert_eq!(store.delete_prefix(b"players/").unwrap(), 2);
            assert_eq!(store.get(b"players/a").unwrap(), None);
        }
        let mut reopened = KeyValueStore::open(&path).unwrap();
        assert_eq!(reopened.get(b"config/mode").unwrap(), Some(b"survival".to_vec()));
        drop(reopened);
        for suffix in [".lock", ".pages", ".pages.wal"] {
            let _ = fs::remove_file(with_suffix(&path, suffix));
        }
    }

    #[test]
    fn key_value_store_prefix_delete_is_bounded_and_repeatable() {
        let path = temp_path("kv-bounded-delete");
        {
            let mut store = KeyValueStore::open(&path).unwrap();
            store.put_many(&[
                (b"players/1".to_vec(), b"one".to_vec()),
                (b"players/2".to_vec(), b"two".to_vec()),
                (b"players/3".to_vec(), b"three".to_vec()),
                (b"players/4".to_vec(), b"four".to_vec()),
                (b"config/mode".to_vec(), b"survival".to_vec()),
            ]).unwrap();

            assert_eq!(store.delete_prefix_bounded(b"players/", 2).unwrap(), 2);
            assert_eq!(store.scan_prefix(b"players/", None).unwrap().len(), 2);
            assert_eq!(store.delete_prefix_bounded(b"players/", 2).unwrap(), 2);
            assert_eq!(store.delete_prefix_bounded(b"players/", 2).unwrap(), 0);
            assert_eq!(store.delete_prefix_bounded(b"config/", 0).unwrap(), 0);
            assert!(store.delete_prefix_bounded(b"config/", 5_001).is_err());
            assert_eq!(store.get(b"config/mode").unwrap(), Some(b"survival".to_vec()));
        }
        let _ = fs::remove_file(with_suffix(&path, ".lock"));
        let _ = fs::remove_file(with_suffix(&path, ".pages"));
        let _ = fs::remove_file(with_suffix(&path, ".pages.wal"));
    }

    #[test]
    fn key_value_store_exposes_cache_configuration_and_metrics() {
        let path = temp_path("kv-page-cache");
        {
            let mut store = KeyValueStore::open_with_cache_capacity(&path, 2).unwrap();
            assert_eq!(store.page_cache_stats().capacity_pages, 2);
            store.put(b"cache/key", b"value").unwrap();
        }

        let mut reopened = KeyValueStore::open_with_cache_capacity(&path, 2).unwrap();
        assert_eq!(reopened.get(b"cache/key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.get(b"cache/key").unwrap(), Some(b"value".to_vec()));
        let warm = reopened.page_cache_stats();
        assert!(warm.hits >= 1, "the repeated key lookup should hit the page cache");
        assert!(warm.misses >= 1, "the first lookup should read and validate the page");

        reopened.set_page_cache_capacity(0).unwrap();
        let cold_before = reopened.page_cache_stats();
        assert_eq!(cold_before.capacity_pages, 0);
        assert_eq!(cold_before.resident_pages, 0);
        reopened.get(b"cache/key").unwrap();
        let cold_after = reopened.page_cache_stats();
        assert_eq!(cold_after.hits, cold_before.hits);
        assert!(cold_after.misses > cold_before.misses);
        drop(reopened);
        for suffix in [".lock", ".pages", ".pages.wal"] {
            let _ = fs::remove_file(with_suffix(&path, suffix));
        }
    }

    #[test]
    fn registry_hierarchy_is_typed_case_insensitive_and_recursive() {
        let path = temp_path("registry");
        {
            let mut registry = Registry::open(&path).unwrap();
            registry.create_hive("SYSTEM").unwrap();
            registry.create_key("SYSTEM\\Database\\Engine").unwrap();
            registry.set_value("system/database/engine", "CacheSize", RegistryValue::Integer(512)).unwrap();
            registry.set_value("SYSTEM\\Database\\Engine", "Mode", RegistryValue::String("EmberTree".into())).unwrap();
            assert_eq!(registry.get_value("System/Database/Engine", "cachesize").unwrap(), Some(RegistryValue::Integer(512)));
            assert_eq!(registry.list_subkeys("SYSTEM/Database").unwrap(), vec!["engine".to_owned()]);
            assert_eq!(registry.list_values("SYSTEM/Database/Engine").unwrap(), vec!["cachesize".to_owned(), "mode".to_owned()]);
            assert!(registry.delete_key("SYSTEM/Database", false).is_err());
            assert!(registry.delete_key("SYSTEM/Database", true).unwrap());
            assert!(!registry.key_exists("SYSTEM/Database/Engine").unwrap());
            assert_eq!(registry.list_hives().unwrap(), vec!["system".to_owned()]);
        }
        let _ = fs::remove_file(with_suffix(&path, ".lock"));
        let _ = fs::remove_file(with_suffix(&path, ".pages"));
        let _ = fs::remove_file(with_suffix(&path, ".pages.wal"));
    }
}
