//! Shared PDB6 logical key/value tree used by catalog, rows and indexes.
//!
//! This is a storage primitive, not yet the SQL Database backend. It provides
//! one CoW root and one redo record for mixed logical mutations.

use super::{extent::{self, ExtentFile}, page_tree};
use super::pdb6::{DatabaseManifest, KEY_COUNTERS, KEY_DATABASE_ROOT, KEY_INDEX, KEY_ROW, KEY_TABLE};
use crate::DbError;
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

const MAX_MUTATIONS: usize = 5_000;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 1024;

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

/// One shared persistent tree for PDB6 catalog, row descriptors and indexes.
pub(crate) struct Pdb6Store {
    pages: ExtentFile,
    wal_path: PathBuf,
    _lock_file: File,
}

impl Pdb6Store {
    pub(crate) fn open(path: impl AsRef<Path>, cache_capacity_pages: usize) -> Result<Self, DbError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(storage_error)?;
            }
        }
        let lock_file = OpenOptions::new().create(true).read(true).write(true)
            .open(sidecar(path, ".pdb6.lock")).map_err(storage_error)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("PDB6 store is already open: {error}")))?;
        let pages_path = sidecar(path, ".pdb6.pages");
        let wal_path = sidecar(path, ".pdb6.wal");
        let mut pages = ExtentFile::open_with_cache_capacity(pages_path, cache_capacity_pages)?;
        page_tree::recover(&mut pages, &wal_path)?;
        page_tree::validate_tree(&mut pages)?;
        Ok(Self { pages, wal_path, _lock_file: lock_file })
    }

    pub(crate) fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        validate_key(key)?;
        page_tree::get(&mut self.pages, key)
    }

    /// Atomically applies mixed logical removals and upserts through one tree
    /// redo frame and one root publication. Upserts take precedence on overlap.
    pub(crate) fn apply_batch<U, R>(&mut self, upserts: U, removals: R) -> Result<(usize, usize), DbError>
    where
        U: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        R: IntoIterator<Item = Vec<u8>>,
    {
        let upserts = upserts.into_iter().take(MAX_MUTATIONS + 1).collect::<Vec<_>>();
        let removals = removals.into_iter().take(MAX_MUTATIONS + 1).collect::<Vec<_>>();
        if upserts.len() + removals.len() > MAX_MUTATIONS {
            return Err(DbError::Constraint(format!("PDB6 batch exceeds {MAX_MUTATIONS} total mutations")));
        }
        let mut unique_upserts = BTreeMap::new();
        let mut total_bytes = 0usize;
        for (key, value) in upserts {
            validate_key(&key)?;
            if value.len() > extent::page_payload_size() {
                return Err(DbError::Constraint("PDB6 inline value exceeds one page; use overflow records".into()));
            }
            total_bytes = total_bytes.checked_add(key.len()).and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| DbError::Constraint("PDB6 batch byte count overflow".into()))?;
            if total_bytes > MAX_BATCH_BYTES {
                return Err(DbError::Constraint("PDB6 batch exceeds the 256 MiB aggregate limit".into()));
            }
            unique_upserts.insert(key, value);
        }
        let mut unique_removals = BTreeSet::new();
        for key in removals {
            validate_key(&key)?;
            unique_removals.insert(key);
        }
        for key in unique_upserts.keys() {
            unique_removals.remove(key);
        }
        if unique_upserts.len() + unique_removals.len() > MAX_MUTATIONS {
            return Err(DbError::Constraint(format!("PDB6 batch exceeds {MAX_MUTATIONS} unique mutations")));
        }
        let upserts = unique_upserts.into_iter().collect::<Vec<_>>();
        let removals = unique_removals.into_iter().collect::<Vec<_>>();
        page_tree::apply_mutations(&mut self.pages, &self.wal_path, &removals, &upserts)
    }

    /// Scans a keyspace prefix in bytewise order, capped at 5,000 entries.
    pub(crate) fn scan_prefix(&mut self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
        if prefix.is_empty() || prefix.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("PDB6 scan prefix must contain 1..=1024 bytes".into()));
        }
        if limit > MAX_MUTATIONS {
            return Err(DbError::Constraint(format!("PDB6 prefix scan limit exceeds {MAX_MUTATIONS}")));
        }
        if limit == 0 { return Ok(Vec::new()); }
        let mut upper = prefix.to_vec();
        let mut pivot = None;
        for index in (0..upper.len()).rev() {
            if upper[index] != u8::MAX {
                upper[index] += 1;
                upper.truncate(index + 1);
                pivot = Some(());
                break;
            }
        }
        let Some(()) = pivot else {
            return Err(DbError::Constraint("PDB6 prefix has no finite upper bound".into()));
        };
        let candidates = page_tree::scan_range_limited(&mut self.pages, prefix, &upper, Some(limit + 1))?;
        Ok(candidates.into_iter().filter(|(key, _)| key.starts_with(prefix)).take(limit).collect())
    }

    pub(crate) fn manifest(&mut self) -> Result<Option<DatabaseManifest>, DbError> {
        self.get(&[KEY_DATABASE_ROOT, 1])?
            .map(|bytes| DatabaseManifest::decode(&bytes))
            .transpose()
    }

    pub(crate) fn set_manifest(&mut self, manifest: DatabaseManifest) -> Result<(), DbError> {
        self.apply_batch(
            [(vec![KEY_DATABASE_ROOT, 1], manifest.encode()?)],
            std::iter::empty::<Vec<u8>>(),
        )?;
        Ok(())
    }
}

fn validate_key(key: &[u8]) -> Result<(), DbError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(DbError::Constraint("PDB6 key must contain 1..=1024 bytes".into()));
    }
    let valid = match key[0] {
        KEY_TABLE | KEY_COUNTERS => key.len() == 9,
        KEY_ROW => key.len() == 17,
        KEY_INDEX => key.len() >= 17,
        KEY_DATABASE_ROOT => key == [KEY_DATABASE_ROOT, 1],
        _ => false,
    };
    if !valid {
        return Err(DbError::Constraint("invalid PDB6 keyspace prefix or key length".into()));
    }
    Ok(())
}

fn storage_error(error: std::io::Error) -> DbError {
    DbError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pdb6::{row_key, table_key};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "postemberdb-pdb6-store-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ))
    }

    fn cleanup(path: &Path) {
        for suffix in [".pdb6.lock", ".pdb6.pages", ".pdb6.wal"] {
            let _ = fs::remove_file(sidecar(path, suffix));
        }
    }

    #[test]
    fn shared_tree_commits_catalog_rows_and_manifest_in_one_root() {
        let path = temp_path();
        {
            let mut store = Pdb6Store::open(&path, 16).unwrap();
            store.set_manifest(DatabaseManifest {
                generation: 1, last_tx_id: 1, next_table_id: 2, table_count: 1,
            }).unwrap();
            let table = table_key(1).unwrap();
            let row = row_key(1, 9).unwrap();
            let (removed, inserted) = store.apply_batch(
                vec![(table.clone(), b"catalog".to_vec()), (row.clone(), b"row-descriptor".to_vec())],
                Vec::<Vec<u8>>::new(),
            ).unwrap();
            assert_eq!((removed, inserted), (0, 2));
            assert_eq!(store.get(&table).unwrap(), Some(b"catalog".to_vec()));
            assert_eq!(store.scan_prefix(&[KEY_ROW], 10).unwrap().len(), 1);
        }
        {
            let mut reopened = Pdb6Store::open(&path, 16).unwrap();
            assert_eq!(reopened.get(&table_key(1).unwrap()).unwrap(), Some(b"catalog".to_vec()));
            assert_eq!(reopened.get(&row_key(1, 9).unwrap()).unwrap(), Some(b"row-descriptor".to_vec()));
            assert_eq!(reopened.manifest().unwrap().unwrap().last_tx_id, 1);
            let (removed, inserted) = reopened.apply_batch(
                vec![(row_key(1, 9).unwrap(), b"updated".to_vec())],
                vec![table_key(1).unwrap(), row_key(1, 9).unwrap()],
            ).unwrap();
            assert_eq!((removed, inserted), (1, 1));
            assert_eq!(reopened.get(&row_key(1, 9).unwrap()).unwrap(), Some(b"updated".to_vec()));
            assert_eq!(reopened.get(&table_key(1).unwrap()).unwrap(), None);
        }
        cleanup(&path);
    }

    #[test]
    fn shared_tree_rejects_invalid_keyspaces_and_oversized_values_before_mutation() {
        let path = temp_path();
        let mut store = Pdb6Store::open(&path, 8).unwrap();
        assert!(store.apply_batch(vec![(vec![0x55, 1], vec![1])], Vec::<Vec<u8>>::new()).is_err());
        assert!(store.apply_batch(vec![(table_key(1).unwrap(), vec![0; extent::page_payload_size() + 1])], Vec::<Vec<u8>>::new()).is_err());
        assert_eq!(store.get(&table_key(1).unwrap()).unwrap(), None);
        drop(store);
        cleanup(&path);
    }
}
