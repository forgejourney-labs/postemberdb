//! Shared PDB6 logical key/value tree used by catalog, rows and indexes.
//!
//! This is a storage primitive, not yet the SQL Database backend. It provides
//! one CoW root and one redo record for mixed logical mutations.

use super::{extent::{self, ExtentFile}, page_tree, participant::{self, PreparedBatch}};
use super::pdb6::{DatabaseManifest, KEY_COUNTERS, KEY_DATABASE_ROOT, KEY_INDEX, KEY_ROW, KEY_TABLE};
use crate::DbError;
use fs2::FileExt;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const MAX_MUTATIONS: usize = 5_000;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 1024;
const MAX_ROW_BYTES: usize = 64 * 1024 * 1024;
const MAX_ROW_BATCH_BYTES: usize = 256 * 1024 * 1024;
const ROW_PAGE_MAGIC: &[u8; 4] = b"P6R1";
const ROW_DESCRIPTOR_MAGIC: &[u8; 4] = b"P6D1";
const ROW_PAGE_HEADER_BYTES: usize = 48;
const SNAPSHOT_MAGIC: &[u8; 4] = b"PSN1";
const MAX_SNAPSHOTS: usize = 10_000;
const MAX_SNAPSHOT_NAME_BYTES: usize = 128;
const ROW_CHUNK_BYTES: usize = extent::page_payload_size() - ROW_PAGE_HEADER_BYTES;
const MAX_ROW_PAGES: usize = (MAX_ROW_BYTES + ROW_CHUNK_BYTES - 1) / ROW_CHUNK_BYTES;

#[derive(Debug, Clone, Copy)]
struct RowPayloadDescriptor {
    first_page_id: u64,
    page_count: u32,
    total_len: u64,
    checksum: u64,
}

impl RowPayloadDescriptor {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(ROW_DESCRIPTOR_MAGIC);
        bytes.extend_from_slice(&self.first_page_id.to_le_bytes());
        bytes.extend_from_slice(&self.page_count.to_le_bytes());
        bytes.extend_from_slice(&self.total_len.to_le_bytes());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() != 32 || &bytes[..4] != ROW_DESCRIPTOR_MAGIC {
            return Err(DbError::Storage("invalid PDB6 row payload descriptor".into()));
        }
        let descriptor = Self {
            first_page_id: u64::from_le_bytes(bytes[4..12].try_into().unwrap()),
            page_count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            total_len: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            checksum: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        };
        let total_len = usize::try_from(descriptor.total_len)
            .map_err(|_| DbError::Storage("PDB6 row length does not fit this platform".into()))?;
        let expected_pages = total_len.max(1).div_ceil(ROW_CHUNK_BYTES);
        if total_len > MAX_ROW_BYTES
            || descriptor.page_count == 0
            || descriptor.page_count as usize != expected_pages
            || descriptor.page_count as usize > MAX_ROW_PAGES
            || descriptor.first_page_id.checked_add(u64::from(descriptor.page_count)).is_none()
        {
            return Err(DbError::Storage("invalid PDB6 row payload extent".into()));
        }
        Ok(descriptor)
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

/// One shared persistent tree for PDB6 catalog, row descriptors and indexes.
pub(crate) struct Pdb6Store {
    pages: ExtentFile,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
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
        Ok(Self { pages, wal_path, snapshot_path: sidecar(path, ".pdb6.snapshots"), _lock_file: lock_file })
    }

    /// Creates a durable named snapshot of the current immutable CoW root.
    /// The snapshot registry is atomically replaced and synced before success is returned.
    pub(crate) fn create_snapshot(&mut self, name: &str) -> Result<(), DbError> {
        validate_snapshot_name(name)?;
        let mut snapshots = read_snapshots(&self.snapshot_path)?;
        if snapshots.contains_key(name) {
            return Err(DbError::Constraint("PDB6 snapshot already exists".into()));
        }
        if snapshots.len() >= MAX_SNAPSHOTS {
            return Err(DbError::Constraint("PDB6 snapshot registry is full".into()));
        }
        snapshots.insert(name.to_string(), self.pages.root_page_id());
        write_snapshots(&self.snapshot_path, &snapshots)
    }

    /// Deletes a named snapshot. Pages remain allocated until a subsequent vacuum.
    pub(crate) fn delete_snapshot(&mut self, name: &str) -> Result<bool, DbError> {
        validate_snapshot_name(name)?;
        let mut snapshots = read_snapshots(&self.snapshot_path)?;
        if snapshots.remove(name).is_none() { return Ok(false); }
        write_snapshots(&self.snapshot_path, &snapshots)?;
        Ok(true)
    }

    pub(crate) fn list_snapshots(&self) -> Result<Vec<String>, DbError> {
        Ok(read_snapshots(&self.snapshot_path)?.into_keys().collect())
    }

    pub(crate) fn prepare_participant_batch(
        &mut self,
        transaction_id: u64,
        removals: &[Vec<u8>],
        upserts: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), DbError> {
        participant::write_prepared(
            &self.wal_path,
            transaction_id,
            &PreparedBatch { removals: removals.to_vec(), upserts: upserts.to_vec() },
        )
    }

    pub(crate) fn read_prepared_value(
        &self,
        transaction_id: u64,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, DbError> {
        let batch = participant::read_prepared(&self.wal_path, transaction_id)?;
        Ok(batch.upserts.into_iter().find(|(candidate, _)| candidate == key).map(|(_, value)| value))
    }

    pub(crate) fn pending_prepared_transactions(&self) -> Result<Vec<u64>, DbError> {
        Ok(participant::list_prepared(&self.wal_path)?.into_iter().collect())
    }

    pub(crate) fn finalize_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        participant::remove_prepared(&self.wal_path, transaction_id)
    }

    pub(crate) fn rollback_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        participant::remove_prepared(&self.wal_path, transaction_id)
    }

    pub(crate) fn has_coordinator_commit(&mut self, transaction_id: u64) -> Result<bool, DbError> {
        Ok(self.get(&super::pdb6::coordinator_tx_key(transaction_id))?.is_some())
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

    /// Applies row payloads and other logical mutations through one PDB6 root.
    ///
    /// Row overflow pages are written and synced first. Their descriptors, catalog
    /// changes, and index mutations are then published together by one CoW root.
    /// A crash before publication can leave unreachable row pages, but cannot
    /// expose a partial logical transaction. Old payload pages are reclaimed by
    /// the future reachability-aware vacuum.
    pub(crate) fn apply_row_batch<U, I, R>(
        &mut self,
        rows: U,
        other_upserts: I,
        other_removals: R,
    ) -> Result<(usize, usize), DbError>
    where
        U: IntoIterator<Item = (u64, u64, Vec<u8>)>,
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
        R: IntoIterator<Item = Vec<u8>>,
    {
        let mut unique_rows = BTreeMap::<(u64, u64), Vec<u8>>::new();
        for (table_id, row_id, payload) in rows {
            let _ = crate::storage::pdb6::row_key(table_id, row_id)?;
            if payload.len() > MAX_ROW_BYTES {
                return Err(DbError::Constraint(format!(
                    "PDB6 SQL row exceeds the {MAX_ROW_BYTES}-byte limit"
                )));
            }
            unique_rows.insert((table_id, row_id), payload);
            if unique_rows.len() > MAX_MUTATIONS {
                return Err(DbError::Constraint(format!(
                    "PDB6 row batch exceeds {MAX_MUTATIONS} rows"
                )));
            }
        }

        let mut unique_upserts = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        for (key, value) in other_upserts {
            validate_key(&key)?;
            if value.len() > extent::page_payload_size() {
                return Err(DbError::Constraint("PDB6 inline value exceeds one page; use overflow records".into()));
            }
            unique_upserts.insert(key, value);
        }
        let mut unique_removals = BTreeSet::new();
        for key in other_removals {
            validate_key(&key)?;
            unique_removals.insert(key);
        }

        let row_keys = unique_rows.keys()
            .map(|(table_id, row_id)| crate::storage::pdb6::row_key(*table_id, *row_id))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if row_keys.iter().any(|key| unique_upserts.contains_key(key)) {
            return Err(DbError::Constraint("PDB6 row key overlaps another batch upsert".into()));
        }
        for key in unique_upserts.keys().chain(row_keys.iter()) {
            unique_removals.remove(key);
        }

        let mutation_count = row_keys.len() + unique_upserts.len() + unique_removals.len();
        if mutation_count > MAX_MUTATIONS {
            return Err(DbError::Constraint(format!(
                "PDB6 row transaction exceeds {MAX_MUTATIONS} total mutations"
            )));
        }
        let row_bytes = unique_rows.values().try_fold(0usize, |total, payload| {
            total.checked_add(payload.len())
                .ok_or_else(|| DbError::Constraint("PDB6 row batch byte count overflow".into()))
        })?;
        let logical_bytes = unique_upserts.iter().try_fold(row_bytes, |total, (key, value)| {
            total.checked_add(key.len()).and_then(|n| n.checked_add(value.len()))
                .ok_or_else(|| DbError::Constraint("PDB6 row transaction byte count overflow".into()))
        })?;
        let logical_bytes = unique_removals.iter().try_fold(logical_bytes, |total, key| {
            total.checked_add(key.len())
                .ok_or_else(|| DbError::Constraint("PDB6 row transaction byte count overflow".into()))
        })?;
        let logical_bytes = row_keys.iter().try_fold(logical_bytes, |total, key| {
            total.checked_add(key.len())
                .ok_or_else(|| DbError::Constraint("PDB6 row transaction byte count overflow".into()))
        })?;
        if logical_bytes > MAX_ROW_BATCH_BYTES {
            return Err(DbError::Constraint(format!(
                "PDB6 row transaction exceeds the {MAX_ROW_BATCH_BYTES}-byte aggregate limit"
            )));
        }

        // Payload pages are durable before their descriptors become reachable.
        let mut row_upserts = Vec::with_capacity(row_keys.len());
        for ((table_id, row_id), payload) in unique_rows {
            let key = crate::storage::pdb6::row_key(table_id, row_id)?;
            let descriptor = self.write_row_payload(table_id, row_id, &payload)?;
            row_upserts.push((key, descriptor));
        }
        let upserts = unique_upserts.into_iter().chain(row_upserts).collect::<Vec<_>>();
        let removals = unique_removals.into_iter().collect::<Vec<_>>();
        self.apply_batch(upserts, removals)
    }

    /// Reads and verifies a PDB6 row payload by stable table and row IDs.
    pub(crate) fn get_row_payload(
        &mut self,
        table_id: u64,
        row_id: u64,
    ) -> Result<Option<Vec<u8>>, DbError> {
        let key = crate::storage::pdb6::row_key(table_id, row_id)?;
        let Some(encoded) = self.get(&key)? else { return Ok(None); };
        let descriptor = RowPayloadDescriptor::decode(&encoded)?;
        let total_len = usize::try_from(descriptor.total_len)
            .map_err(|_| DbError::Storage("PDB6 row length does not fit this platform".into()))?;
        let mut output = Vec::with_capacity(total_len);

        for page_index in 0..descriptor.page_count {
            let page_id = descriptor.first_page_id.checked_add(u64::from(page_index))
                .ok_or_else(|| DbError::Storage("PDB6 row page ID overflow".into()))?;
            let page = self.pages.read_page(page_id)?;
            if page.len() < ROW_PAGE_HEADER_BYTES || &page[..4] != ROW_PAGE_MAGIC {
                return Err(DbError::Storage(format!("invalid PDB6 row page {page_id}")));
            }
            let stored_table_id = u64::from_le_bytes(page[4..12].try_into().unwrap());
            let stored_row_id = u64::from_le_bytes(page[12..20].try_into().unwrap());
            let stored_index = u32::from_le_bytes(page[20..24].try_into().unwrap());
            let stored_count = u32::from_le_bytes(page[24..28].try_into().unwrap());
            let stored_total_len = u64::from_le_bytes(page[28..36].try_into().unwrap());
            let stored_checksum = u64::from_le_bytes(page[36..44].try_into().unwrap());
            let chunk_len = u32::from_le_bytes(page[44..48].try_into().unwrap()) as usize;
            if stored_table_id != table_id
                || stored_row_id != row_id
                || stored_index != page_index
                || stored_count != descriptor.page_count
                || stored_total_len != descriptor.total_len
                || stored_checksum != descriptor.checksum
                || chunk_len != page.len() - ROW_PAGE_HEADER_BYTES
                || chunk_len > ROW_CHUNK_BYTES
                || chunk_len != total_len.saturating_sub(output.len()).min(ROW_CHUNK_BYTES)
            {
                return Err(DbError::Storage(format!("PDB6 row page metadata mismatch at page {page_id}")));
            }
            if output.len().checked_add(chunk_len).is_none_or(|length| length > total_len) {
                return Err(DbError::Storage("PDB6 row chunks exceed the declared row length".into()));
            }
            output.extend_from_slice(&page[ROW_PAGE_HEADER_BYTES..]);
        }
        if output.len() != total_len || super::checksum(&output) != descriptor.checksum {
            return Err(DbError::Storage("PDB6 row length or checksum mismatch".into()));
        }
        Ok(Some(output))
    }

    /// Returns a bounded page of stable row IDs in ascending order.
    pub(crate) fn scan_row_ids(
        &mut self,
        table_id: u64,
        after_exclusive: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, DbError> {
        let _ = crate::storage::pdb6::table_key(table_id)?;
        let mut prefix = vec![KEY_ROW];
        prefix.extend_from_slice(&table_id.to_be_bytes());
        let cursor = after_exclusive
            .map(|row_id| crate::storage::pdb6::row_key(table_id, row_id))
            .transpose()?;
        self.scan_prefix_after(&prefix, cursor.as_deref(), limit)?
            .into_iter()
            .map(|(key, _)| {
                if key.len() != 17 {
                    return Err(DbError::Storage("invalid PDB6 row key length during scan".into()));
                }
                Ok(u64::from_be_bytes(key[9..17].try_into().unwrap()))
            })
            .collect()
    }

    /// Reclaims unreachable tree nodes and orphaned P6R1 overflow pages.
    /// Every live row descriptor is validated and its full contiguous payload
    /// extent is pinned before any page is released. Unknown page kinds remain
    /// allocated. The exclusive PDB6 lock prevents another process publishing
    /// a root during this pass; retained snapshots/readers are not yet supported.
    pub(crate) fn vacuum_unreachable_tree_pages(&mut self) -> Result<u64, DbError> {
        let snapshots = read_snapshots(&self.snapshot_path)?;
        let mut reachable = page_tree::reachable_page_ids(&mut self.pages)?;
        for root in snapshots.values().flatten().copied() {
            reachable.extend(page_tree::reachable_page_ids_from_root(&mut self.pages, root)?);
        }
        // Snapshot roots can retain older row descriptors. Until descriptor
        // traversal is available for arbitrary roots, conservatively retain every
        // overflow page whenever at least one named snapshot exists.
        let retain_all_overflow = !snapshots.is_empty();
        let row_prefix = [KEY_ROW];
        let mut live_overflow = BTreeSet::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let batch = self.scan_prefix_after(&row_prefix, cursor.as_deref(), 1_000)?;
            if batch.is_empty() { break; }
            for (key, encoded_descriptor) in &batch {
                if key.len() != 17 {
                    return Err(DbError::Storage("invalid PDB6 row key during overflow vacuum".into()));
                }
                let table_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let row_id = u64::from_be_bytes(key[9..17].try_into().unwrap());
                let descriptor = RowPayloadDescriptor::decode(encoded_descriptor)?;
                let total_len = usize::try_from(descriptor.total_len)
                    .map_err(|_| DbError::Storage("PDB6 row length does not fit this platform".into()))?;
                for page_index in 0..descriptor.page_count {
                    let page_id = descriptor.first_page_id.checked_add(u64::from(page_index))
                        .ok_or_else(|| DbError::Storage("PDB6 overflow page ID overflow during vacuum".into()))?;
                    if reachable.contains(&page_id) || !live_overflow.insert(page_id) {
                        return Err(DbError::Storage("PDB6 overflow extent overlaps a tree node or another row".into()));
                    }
                    let page = self.pages.read_page(page_id)?;
                    if page.len() < ROW_PAGE_HEADER_BYTES || &page[..4] != ROW_PAGE_MAGIC {
                        return Err(DbError::Storage(format!("invalid live PDB6 overflow page {page_id}")));
                    }
                    let stored_table_id = u64::from_le_bytes(page[4..12].try_into().unwrap());
                    let stored_row_id = u64::from_le_bytes(page[12..20].try_into().unwrap());
                    let stored_index = u32::from_le_bytes(page[20..24].try_into().unwrap());
                    let stored_count = u32::from_le_bytes(page[24..28].try_into().unwrap());
                    let stored_total_len = u64::from_le_bytes(page[28..36].try_into().unwrap());
                    let stored_checksum = u64::from_le_bytes(page[36..44].try_into().unwrap());
                    let chunk_len = u32::from_le_bytes(page[44..48].try_into().unwrap()) as usize;
                    let expected_chunk_len = total_len.saturating_sub(page_index as usize * ROW_CHUNK_BYTES).min(ROW_CHUNK_BYTES);
                    if stored_table_id != table_id || stored_row_id != row_id
                        || stored_index != page_index || stored_count != descriptor.page_count
                        || stored_total_len != descriptor.total_len || stored_checksum != descriptor.checksum
                        || chunk_len != page.len() - ROW_PAGE_HEADER_BYTES
                        || chunk_len != expected_chunk_len || chunk_len > ROW_CHUNK_BYTES {
                        return Err(DbError::Storage(format!("live PDB6 overflow page metadata mismatch at page {page_id}")));
                    }
                }
            }
            cursor = batch.last().map(|(key, _)| key.clone());
            if batch.len() < 1_000 { break; }
        }

        let mut dead = Vec::new();
        for (start, count) in self.pages.allocated_extents() {
            let end = start.checked_add(count).ok_or_else(|| DbError::Storage("allocated extent end overflow during vacuum".into()))?;
            for page_id in start..end {
                if reachable.contains(&page_id) || live_overflow.contains(&page_id) { continue; }
                let payload = self.pages.read_page(page_id)?;
                if payload.starts_with(b"EBL1") || payload.starts_with(b"EBI1") || (!retain_all_overflow && payload.starts_with(ROW_PAGE_MAGIC)) {
                    dead.push(page_id);
                }
            }
        }
        if dead.is_empty() { return Ok(0); }
        dead.sort_unstable();
        let reclaimed = dead.len() as u64;
        let mut range_start = dead[0];
        let mut previous = dead[0];
        for page_id in dead.into_iter().skip(1) {
            if page_id == previous + 1 { previous = page_id; continue; }
            self.pages.release_extent(range_start, previous - range_start + 1)?;
            range_start = page_id;
            previous = page_id;
        }
        self.pages.release_extent(range_start, previous - range_start + 1)?;
        Ok(reclaimed)
    }

    fn write_row_payload(
        &mut self,
        table_id: u64,
        row_id: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, DbError> {
        let page_count = payload.len().max(1).div_ceil(ROW_CHUNK_BYTES);
        if page_count > MAX_ROW_PAGES {
            return Err(DbError::Constraint("PDB6 row requires too many pages".into()));
        }
        let checksum = super::checksum(payload);
        let first_page_id = self.pages.allocate_extent(page_count as u64)?;
        let mut pages = Vec::with_capacity(page_count);
        for page_index in 0..page_count {
            let start = page_index * ROW_CHUNK_BYTES;
            let end = payload.len().min(start + ROW_CHUNK_BYTES);
            let chunk = &payload[start..end];
            let mut page = Vec::with_capacity(ROW_PAGE_HEADER_BYTES + chunk.len());
            page.extend_from_slice(ROW_PAGE_MAGIC);
            page.extend_from_slice(&table_id.to_le_bytes());
            page.extend_from_slice(&row_id.to_le_bytes());
            page.extend_from_slice(&(page_index as u32).to_le_bytes());
            page.extend_from_slice(&(page_count as u32).to_le_bytes());
            page.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            page.extend_from_slice(&checksum.to_le_bytes());
            page.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
            page.extend_from_slice(chunk);
            pages.push(page);
        }
        self.pages.write_pages(first_page_id, &pages)?;
        Ok(RowPayloadDescriptor {
            first_page_id,
            page_count: page_count as u32,
            total_len: payload.len() as u64,
            checksum,
        }.encode())
    }

    /// Scans a keyspace prefix in bytewise order, capped at 5,000 entries.
    pub(crate) fn scan_prefix(&mut self, prefix: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
        self.scan_prefix_after(prefix, None, limit)
    }

    /// Reads one bounded keyset page after an exclusive key cursor.
    ///
    /// Supply the final returned key as the next cursor. The cursor must belong
    /// to the prefix. This avoids offset scans and bounds memory per page.
    pub(crate) fn scan_prefix_after(
        &mut self,
        prefix: &[u8],
        after_exclusive: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
        if prefix.is_empty() || prefix.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("PDB6 scan prefix must contain 1..=1024 bytes".into()));
        }
        if limit > MAX_MUTATIONS {
            return Err(DbError::Constraint(format!("PDB6 prefix scan limit exceeds {MAX_MUTATIONS}")));
        }
        if limit == 0 { return Ok(Vec::new()); }

        let upper = prefix_successor(prefix)
            .ok_or_else(|| DbError::Constraint("PDB6 prefix has no finite upper bound".into()))?;
        let lower = match after_exclusive {
            Some(cursor) => {
                if !cursor.starts_with(prefix) || cursor.len() > MAX_KEY_BYTES {
                    return Err(DbError::Constraint("PDB6 scan cursor does not belong to the requested prefix".into()));
                }
                cursor.to_vec()
            }
            None => prefix.to_vec(),
        };
        if lower >= upper { return Ok(Vec::new()); }

        // Keep the cursor inclusive at the tree layer, then discard the exact
        // cursor key. This preserves variable-length keys that extend it.
        let candidates = page_tree::scan_range_limited(&mut self.pages, &lower, &upper, Some(limit + 2))?;
        Ok(candidates.into_iter()
            .filter(|(key, _)| key.starts_with(prefix)
                && after_exclusive.is_none_or(|cursor| key.as_slice() > cursor))
            .take(limit)
            .collect())
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

fn validate_snapshot_name(name: &str) -> Result<(), DbError> {
    if name.is_empty() || name.len() > MAX_SNAPSHOT_NAME_BYTES
        || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte)) {
        return Err(DbError::Constraint("snapshot names must contain 1..=128 ASCII letters, digits, dots, underscores or hyphens".into()));
    }
    Ok(())
}

fn read_snapshots(path: &Path) -> Result<BTreeMap<String, Option<u64>>, DbError> {
    if !path.exists() { return Ok(BTreeMap::new()); }
    let bytes = fs::read(path).map_err(storage_error)?;
    if bytes.len() < 16 || &bytes[..4] != SNAPSHOT_MAGIC {
        return Err(DbError::Storage("invalid PDB6 snapshot registry header".into()));
    }
    let stored_checksum = u64::from_le_bytes(bytes[bytes.len()-8..].try_into().unwrap());
    if super::checksum(&bytes[..bytes.len()-8]) != stored_checksum {
        return Err(DbError::Storage("PDB6 snapshot registry checksum mismatch".into()));
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if count > MAX_SNAPSHOTS { return Err(DbError::Storage("PDB6 snapshot registry exceeds its entry limit".into())); }
    let mut cursor = 8usize;
    let mut snapshots = BTreeMap::new();
    for _ in 0..count {
        if cursor + 2 > bytes.len()-8 { return Err(DbError::Storage("truncated PDB6 snapshot name length".into())); }
        let name_len = u16::from_le_bytes(bytes[cursor..cursor+2].try_into().unwrap()) as usize;
        cursor += 2;
        if name_len == 0 || name_len > MAX_SNAPSHOT_NAME_BYTES || cursor + name_len + 8 > bytes.len()-8 {
            return Err(DbError::Storage("invalid PDB6 snapshot registry entry length".into()));
        }
        let name = std::str::from_utf8(&bytes[cursor..cursor+name_len])
            .map_err(|_| DbError::Storage("PDB6 snapshot name is not UTF-8".into()))?.to_string();
        validate_snapshot_name(&name)?;
        cursor += name_len;
        let root = u64::from_le_bytes(bytes[cursor..cursor+8].try_into().unwrap());
        cursor += 8;
        if snapshots.insert(name, (root != u64::MAX).then_some(root)).is_some() {
            return Err(DbError::Storage("duplicate PDB6 snapshot name".into()));
        }
    }
    if cursor != bytes.len()-8 { return Err(DbError::Storage("trailing bytes in PDB6 snapshot registry".into())); }
    Ok(snapshots)
}

fn write_snapshots(path: &Path, snapshots: &BTreeMap<String, Option<u64>>) -> Result<(), DbError> {
    if snapshots.len() > MAX_SNAPSHOTS { return Err(DbError::Constraint("PDB6 snapshot registry is full".into())); }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&(snapshots.len() as u32).to_le_bytes());
    for (name, root) in snapshots {
        validate_snapshot_name(name)?;
        bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&root.unwrap_or(u64::MAX).to_le_bytes());
    }
    let checksum = super::checksum(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    let mut temp_name = path.as_os_str().to_os_string();
    temp_name.push(format!(".tmp-{}", std::process::id()));
    let temp = PathBuf::from(temp_name);
    let result = (|| {
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temp).map_err(storage_error)?;
        file.write_all(&bytes).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        fs::rename(&temp, path).map_err(storage_error)?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent).and_then(|directory| directory.sync_all()).map_err(storage_error)?;
        }
        Ok(())
    })();
    if result.is_err() { let _ = fs::remove_file(&temp); }
    result
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    for index in (0..next.len()).rev() {
        if next[index] != u8::MAX {
            next[index] += 1;
            next.truncate(index + 1);
            return Some(next);
        }
    }
    None
}

fn validate_key(key: &[u8]) -> Result<(), DbError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(DbError::Constraint("PDB6 key must contain 1..=1024 bytes".into()));
    }
    let valid = match key[0] {
        KEY_TABLE | KEY_COUNTERS | super::pdb6::KEY_COORDINATOR => key.len() == 9,
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
        for suffix in [".pdb6.lock", ".pdb6.pages", ".pdb6.wal", ".pdb6.snapshots", ".pdb6.snapshots.tmp"] {
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
    fn prefix_scan_uses_exclusive_keyset_cursor_across_large_keyspaces() {
        let path = temp_path();
        {
            let mut store = Pdb6Store::open(&path, 32).unwrap();
            let first = (1..=5_000u64).map(|id| {
                (row_key(7, id).unwrap(), id.to_le_bytes().to_vec())
            }).collect::<Vec<_>>();
            store.apply_batch(first, Vec::<Vec<u8>>::new()).unwrap();
            let second = (5_001..=6_200u64).map(|id| {
                (row_key(7, id).unwrap(), id.to_le_bytes().to_vec())
            }).collect::<Vec<_>>();
            store.apply_batch(second, Vec::<Vec<u8>>::new()).unwrap();

            let mut prefix = vec![KEY_ROW];
            prefix.extend_from_slice(&7u64.to_be_bytes());
            let mut cursor: Option<Vec<u8>> = None;
            let mut observed = Vec::new();
            loop {
                let page = store.scan_prefix_after(&prefix, cursor.as_deref(), 777).unwrap();
                if page.is_empty() { break; }
                cursor = Some(page.last().unwrap().0.clone());
                observed.extend(page.into_iter().map(|(_, value)| u64::from_le_bytes(value.try_into().unwrap())));
            }
            assert_eq!(observed, (1..=6_200u64).collect::<Vec<_>>());
            assert!(store.scan_prefix_after(&prefix, cursor.as_deref(), 777).unwrap().is_empty());
            assert!(store.scan_prefix_after(&prefix, Some(&table_key(7).unwrap()), 1).is_err());

            // Variable-length index keys may extend the cursor byte-for-byte.
            let index_prefix = crate::storage::pdb6::index_prefix(3, 4).unwrap();
            let mut key_a = index_prefix.clone();
            key_a.push(0x11);
            let mut key_b = key_a.clone();
            key_b.push(0x00);
            let mut key_c = key_b.clone();
            key_c.push(0x00);
            store.apply_batch(
                vec![(key_a.clone(), vec![1]), (key_b.clone(), vec![2]), (key_c.clone(), vec![3])],
                Vec::<Vec<u8>>::new(),
            ).unwrap();
            let first = store.scan_prefix_after(&index_prefix, None, 1).unwrap();
            assert_eq!(first, vec![(key_a.clone(), vec![1])]);
            let second = store.scan_prefix_after(&index_prefix, Some(&first[0].0), 1).unwrap();
            assert_eq!(second, vec![(key_b.clone(), vec![2])]);
            let third = store.scan_prefix_after(&index_prefix, Some(&second[0].0), 1).unwrap();
            assert_eq!(third, vec![(key_c, vec![3])]);
        }
        cleanup(&path);
    }

    #[test]
    fn pdb6_row_payloads_share_one_root_with_catalog_and_index_mutations() {
        let path = temp_path();
        let payload = (0..20_000usize).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        let index_key = crate::storage::pdb6::index_key(12, 3, b"player-uuid").unwrap();
        {
            let mut store = Pdb6Store::open(&path, 16).unwrap();
            let (removed, inserted) = store.apply_row_batch(
                vec![(12, 41, payload.clone()), (12, 42, Vec::new())],
                vec![(index_key.clone(), 41u64.to_le_bytes().to_vec())],
                Vec::<Vec<u8>>::new(),
            ).unwrap();
            assert_eq!((removed, inserted), (0, 3));
            assert_eq!(store.get_row_payload(12, 41).unwrap(), Some(payload.clone()));
            assert_eq!(store.get_row_payload(12, 42).unwrap(), Some(Vec::new()));
            assert_eq!(store.get_row_payload(12, 99).unwrap(), None);
            assert_eq!(store.scan_row_ids(12, None, 10).unwrap(), vec![41, 42]);
            assert_eq!(store.scan_row_ids(12, Some(41), 10).unwrap(), vec![42]);
            assert_eq!(store.get(&index_key).unwrap(), Some(41u64.to_le_bytes().to_vec()));
        }
        {
            let mut reopened = Pdb6Store::open(&path, 16).unwrap();
            assert_eq!(reopened.get_row_payload(12, 41).unwrap(), Some(payload));
            let (removed, inserted) = reopened.apply_row_batch(
                vec![(12, 41, b"replacement".to_vec())],
                Vec::<(Vec<u8>, Vec<u8>)>::new(),
                vec![crate::storage::pdb6::row_key(12, 42).unwrap(), index_key.clone()],
            ).unwrap();
            assert_eq!((removed, inserted), (2, 1));
            assert_eq!(reopened.get_row_payload(12, 41).unwrap(), Some(b"replacement".to_vec()));
            assert_eq!(reopened.get_row_payload(12, 42).unwrap(), None);
            assert_eq!(reopened.get(&index_key).unwrap(), None);
            assert_eq!(reopened.scan_row_ids(12, None, 10).unwrap(), vec![41]);
        }
        cleanup(&path);
    }

    #[test]
    fn vacuum_reclaims_only_unreachable_tree_nodes_and_preserves_row_overflow_pages() {
        let path = temp_path();
        let payload = vec![0x5a; 80_000];
        {
            let mut store = Pdb6Store::open(&path, 32).unwrap();
            store.apply_row_batch(vec![(5, 9, payload.clone())], Vec::<(Vec<u8>, Vec<u8>)>::new(), Vec::<Vec<u8>>::new()).unwrap();
            store.apply_batch((0..100u64).map(|id| (table_key(id + 1).unwrap(), vec![id as u8])).collect::<Vec<_>>(), Vec::<Vec<u8>>::new()).unwrap();
            store.apply_batch(vec![(table_key(2).unwrap(), b"replaced".to_vec())], Vec::<Vec<u8>>::new()).unwrap();
            let reclaimed = store.vacuum_unreachable_tree_pages().unwrap();
            assert!(reclaimed > 0);
            assert_eq!(store.get_row_payload(5, 9).unwrap(), Some(payload));
        }
        cleanup(&path);
    }

    #[test]
    fn vacuum_reclaims_orphaned_overflow_pages_but_preserves_live_descriptors() {
        let path = temp_path();
        let payload = vec![0x6b; 90_000];
        {
            let mut store = Pdb6Store::open(&path, 32).unwrap();
            store.apply_row_batch(
                vec![(77, 9001, payload.clone())],
                Vec::<(Vec<u8>, Vec<u8>)>::new(),
                Vec::<Vec<u8>>::new(),
            ).unwrap();
            let descriptor = RowPayloadDescriptor::decode(
                &store.get(&crate::storage::pdb6::row_key(77, 9001).unwrap()).unwrap().unwrap()
            ).unwrap();
            assert_eq!(store.get_row_payload(77, 9001).unwrap(), Some(payload));
            store.apply_batch(Vec::<(Vec<u8>, Vec<u8>)>::new(), vec![
                crate::storage::pdb6::row_key(77, 9001).unwrap()
            ]).unwrap();
            let reclaimed = store.vacuum_unreachable_tree_pages().unwrap();
            assert!(reclaimed >= u64::from(descriptor.page_count));
            assert_eq!(store.get_row_payload(77, 9001).unwrap(), None);
        }
        cleanup(&path);
    }

    #[test]
    fn named_snapshots_survive_reopen_and_pin_tree_and_overflow_pages_until_vacuum() {
        let path = temp_path();
        let payload = vec![0x4d; 100_000];
        let old_root_pages;
        let overflow_descriptor;
        {
            let mut store = Pdb6Store::open(&path, 32).unwrap();
            store.apply_row_batch(
                vec![(91, 7001, payload.clone())],
                Vec::<(Vec<u8>, Vec<u8>)>::new(),
                Vec::<Vec<u8>>::new(),
            ).unwrap();
            old_root_pages = page_tree::reachable_page_ids(&mut store.pages).unwrap();
            overflow_descriptor = RowPayloadDescriptor::decode(
                &store.get(&crate::storage::pdb6::row_key(91, 7001).unwrap()).unwrap().unwrap()
            ).unwrap();
            store.create_snapshot("before-delete").unwrap();
            assert!(store.create_snapshot("before-delete").is_err());
            assert_eq!(store.list_snapshots().unwrap(), vec!["before-delete"]);
            store.apply_batch(
                Vec::<(Vec<u8>, Vec<u8>)>::new(),
                vec![crate::storage::pdb6::row_key(91, 7001).unwrap()],
            ).unwrap();
            assert_eq!(store.get_row_payload(91, 7001).unwrap(), None);
            assert!(store.vacuum_unreachable_tree_pages().unwrap() > 0);
            for page_id in &old_root_pages { assert!(store.pages.read_page(*page_id).is_ok()); }
            for index in 0..overflow_descriptor.page_count {
                assert!(store.pages.read_page(overflow_descriptor.first_page_id + u64::from(index)).is_ok());
            }
        }
        {
            let mut reopened = Pdb6Store::open(&path, 32).unwrap();
            assert_eq!(reopened.list_snapshots().unwrap(), vec!["before-delete"]);
            for page_id in &old_root_pages { assert!(reopened.pages.read_page(*page_id).is_ok()); }
            assert!(reopened.delete_snapshot("before-delete").unwrap());
            assert!(!reopened.delete_snapshot("before-delete").unwrap());
            assert!(reopened.vacuum_unreachable_tree_pages().unwrap() > 0);
            for page_id in &old_root_pages { assert!(reopened.pages.read_page(*page_id).is_err()); }
            for index in 0..overflow_descriptor.page_count {
                assert!(reopened.pages.read_page(overflow_descriptor.first_page_id + u64::from(index)).is_err());
            }
            assert!(reopened.list_snapshots().unwrap().is_empty());
            assert!(reopened.create_snapshot("invalid/name").is_err());
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
