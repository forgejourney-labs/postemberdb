//! File-backed fixed pages and a durable free-extent map.
//!
//! This is a storage primitive, not yet the Database/EmberTree page backend.
//! Callers must publish new roots only after page and allocator metadata writes
//! are durable, and must not overwrite live pages until the WAL-backed in-place
//! update protocol is implemented.

use super::DbError;
use super::cache::{PageCache, DEFAULT_PAGE_CACHE_CAPACITY};
use crate::PageCacheStats;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
 
const PAGE_SIZE: usize = super::PAGE_SIZE;
const METADATA_SLOTS: u64 = 2;
const METADATA_BYTES: u64 = METADATA_SLOTS * PAGE_SIZE as u64;
const PAGE_HEADER_SIZE: usize = 32;
const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;
const METADATA_HEADER_SIZE: usize = 44;
const EXTENT_ENTRY_SIZE: usize = 16;
const MAX_FREE_EXTENTS: usize = (PAGE_SIZE - METADATA_HEADER_SIZE) / EXTENT_ENTRY_SIZE;

pub(super) const fn page_payload_size() -> usize { PAGE_PAYLOAD_SIZE }
const METADATA_MAGIC: &[u8; 4] = b"EAM3";
const DATA_PAGE_MAGIC: &[u8; 4] = b"EPG1";
const FORMAT_VERSION: u32 = 3;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataFailpoint {
    AfterPrimarySync,
    AfterMirrorSync,
}

#[cfg(test)]
std::thread_local! {
    static METADATA_FAILPOINT: std::cell::Cell<Option<MetadataFailpoint>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn set_metadata_failpoint(failpoint: Option<MetadataFailpoint>) {
    METADATA_FAILPOINT.with(|slot| slot.set(failpoint));
}

#[cfg(test)]
fn take_metadata_failpoint(expected: MetadataFailpoint) -> bool {
    METADATA_FAILPOINT.with(|slot| {
        if slot.get() == Some(expected) {
            slot.set(None);
            true
        } else {
            false
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AllocatorMetadata {
    generation: u64,
    next_page_id: u64,
    root_page_id: Option<u64>,
    free_extents: BTreeMap<u64, u64>,
}

/// Durable allocator statistics. Page IDs are logical IDs, not byte offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExtentStats {
    pub next_page_id: u64,
    pub allocated_pages: u64,
    pub free_pages: u64,
    pub free_extent_count: usize,
}

/// A single-owner page file with two alternating metadata slots.
#[derive(Debug)]
pub(crate) struct ExtentFile {
    file: File,
    metadata: AllocatorMetadata,
    poisoned: bool,
    page_cache: PageCache,
}

impl ExtentFile {
    fn ensure_healthy(&self) -> Result<(), DbError> {
        if self.poisoned {
            return Err(DbError::Storage(
                "extent file has an uncertain write outcome; close and reopen it before continuing".into(),
            ));
        }
        Ok(())
    }

    fn repair_metadata_slots(&mut self, keep_slot: u64) -> Result<(), DbError> {
        let encoded = encode_metadata(&self.metadata)?;
        let repair_slot = (keep_slot + 1) % METADATA_SLOTS;

        // Keep the selected valid copy untouched until the other slot has been
        // repaired and synced. If this repair is interrupted, one valid copy
        // still contains the selected state.
        write_at(&mut self.file, repair_slot * PAGE_SIZE as u64, &encoded)?;
        self.file.sync_all().map_err(storage_error)?;
        write_at(&mut self.file, keep_slot * PAGE_SIZE as u64, &encoded)?;
        self.file.sync_all().map_err(storage_error)?;
        Ok(())
    }

    /// Opens or initializes a page file with the default bounded page cache.
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_cache_capacity(path, DEFAULT_PAGE_CACHE_CAPACITY)
    }

    /// Opens or initializes a page file with a caller-selected cache capacity.
    /// A single process must own the file. Capacity is measured in decoded pages.
    pub(crate) fn open_with_cache_capacity(
        path: impl AsRef<Path>,
        cache_capacity_pages: usize,
    ) -> Result<Self, DbError> {
        let page_cache = PageCache::new(cache_capacity_pages)?;
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(storage_error)?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(storage_error)?;
        let file_len = file.metadata().map_err(storage_error)?.len();

        if file_len == 0 {
            file.set_len(METADATA_BYTES).map_err(storage_error)?;
            let mut store = Self {
                file,
                metadata: AllocatorMetadata {
                    generation: 0,
                    next_page_id: 0,
                    root_page_id: None,
                    free_extents: BTreeMap::new(),
                },
                poisoned: false,
                page_cache,
            };
            let initial = store.metadata.clone();
            store.commit_metadata(initial)?;
            return Ok(store);
        }

        if file_len < METADATA_BYTES {
            return Err(DbError::Storage("extent file is shorter than its metadata area".into()));
        }

        let slot0 = read_metadata_slot(&mut file, 0);
        let slot1 = read_metadata_slot(&mut file, 1);
        let (metadata, keep_slot, repair_needed) = match (&slot0, &slot1) {
            (Ok(Some(left)), Ok(Some(right))) => {
                if left.generation > right.generation {
                    (left.clone(), 0, true)
                } else if right.generation > left.generation {
                    (right.clone(), 1, true)
                } else if left == right {
                    (left.clone(), 0, false)
                } else {
                    return Err(DbError::Storage(
                        "extent metadata slots disagree at the same generation".into(),
                    ));
                }
            }
            (Ok(Some(valid)), Ok(None)) | (Ok(Some(valid)), Err(_)) => {
                (valid.clone(), 0, true)
            }
            (Ok(None), Ok(Some(valid))) | (Err(_), Ok(Some(valid))) => {
                (valid.clone(), 1, true)
            }
            (Ok(None), Ok(None)) => {
                return Err(DbError::Storage("both extent metadata slots are empty".into()));
            }
            (Ok(None), Err(error)) | (Err(error), Ok(None)) => {
                return Err(DbError::Storage(format!(
                    "one extent metadata slot is empty and the other is invalid: {error}"
                )));
            }
            (Err(left), Err(right)) => {
                return Err(DbError::Storage(format!(
                    "both extent metadata slots are invalid (slot 0: {left}; slot 1: {right})"
                )));
            }
        };

        validate_metadata(&metadata)?;
        let required_len = page_offset(metadata.next_page_id)?;
        if file_len < required_len {
            return Err(DbError::Storage(
                "extent file is truncated below its committed high-water mark".into(),
            ));
        }

        let mut store = Self { file, metadata, poisoned: false, page_cache };
        if repair_needed {
            store.repair_metadata_slots(keep_slot)?;
        }
        Ok(store)
    }

    /// Reserves a contiguous range of page IDs using first-fit extent allocation.
    /// A crash after this call but before the pages are written can leak reserved
    /// space, but cannot cause the allocator to hand out the same IDs twice.
    pub(crate) fn allocate_extent(&mut self, page_count: u64) -> Result<u64, DbError> {
        self.ensure_healthy()?;
        if page_count == 0 {
            return Err(DbError::Storage("cannot allocate an empty extent".into()));
        }
        let mut next = self.metadata.clone();
        let start = reserve_extent(&mut next, page_count)?;
        // Extend the file before publishing a larger high-water mark. If the
        // metadata commit fails, the extra tail is harmless and may be reused.
        let required_len = page_offset(next.next_page_id)?;
        if self.file.metadata().map_err(storage_error)?.len() < required_len {
            if let Err(error) = self.file.set_len(required_len) {
                self.poisoned = true;
                return Err(storage_error(error));
            }
            if let Err(error) = self.file.sync_data() {
                self.poisoned = true;
                return Err(storage_error(error));
            }
        }
        self.commit_metadata(next)?;
        Ok(start)
    }

    /// Allocates one page, writes and syncs its contents, then commits allocator
    /// metadata. This order ensures an uncommitted allocation is never returned
    /// to a caller before its bytes and allocation state are durable.
    pub(crate) fn allocate_page(&mut self, payload: &[u8]) -> Result<u64, DbError> {
        self.ensure_healthy()?;
        if payload.len() > PAGE_PAYLOAD_SIZE {
            return Err(DbError::Storage(format!(
                "page payload exceeds the {PAGE_PAYLOAD_SIZE}-byte limit"
            )));
        }

        let generation = self.metadata.generation.checked_add(1)
            .ok_or_else(|| DbError::Storage("extent metadata generation exhausted".into()))?;
        let mut next = self.metadata.clone();
        let page_id = reserve_extent(&mut next, 1)?;
        let encoded = encode_data_page(page_id, generation, payload);
        if let Err(error) = write_at(&mut self.file, page_offset(page_id)?, &encoded) {
            self.poisoned = true;
            return Err(error);
        }
        if let Err(error) = self.file.sync_data() {
            self.poisoned = true;
            return Err(storage_error(error));
        }
        self.commit_metadata(next)?;
        self.page_cache.insert(page_id, payload.to_vec());
        Ok(page_id)
    }

    /// Writes an allocated page. Until WAL-backed in-place updates exist, callers
    /// should only use this for reserved pages that are not reachable from a root.
    pub(crate) fn write_page(&mut self, page_id: u64, payload: &[u8]) -> Result<(), DbError> {
        self.ensure_healthy()?;
        if payload.len() > PAGE_PAYLOAD_SIZE {
            return Err(DbError::Storage(format!(
                "page payload exceeds the {PAGE_PAYLOAD_SIZE}-byte limit"
            )));
        }
        if !is_allocated(&self.metadata, page_id) {
            return Err(DbError::Storage(format!("page {page_id} is not allocated")));
        }

        let generation = self.metadata.generation;
        let encoded = encode_data_page(page_id, generation, payload);
        if let Err(error) = write_at(&mut self.file, page_offset(page_id)?, &encoded) {
            self.poisoned = true;
            return Err(error);
        }
        if let Err(error) = self.file.sync_data() {
            self.poisoned = true;
            return Err(storage_error(error));
        }
        self.page_cache.insert(page_id, payload.to_vec());
        Ok(())
    }

    /// Writes a contiguous run of already-reserved pages and performs one data sync.
    /// This is used by overflow-record writers to avoid one fsync per chunk.
    pub(crate) fn write_pages(&mut self, start_page_id: u64, payloads: &[Vec<u8>]) -> Result<(), DbError> {
        self.ensure_healthy()?;
        if payloads.is_empty() {
            return Err(DbError::Storage("cannot write an empty page run".into()));
        }
        let end = start_page_id.checked_add(payloads.len() as u64)
            .ok_or_else(|| DbError::Storage("page run end overflow".into()))?;
        for (index, payload) in payloads.iter().enumerate() {
            if payload.len() > PAGE_PAYLOAD_SIZE {
                return Err(DbError::Storage(format!("page payload exceeds the {PAGE_PAYLOAD_SIZE}-byte limit")));
            }
            let page_id = start_page_id + index as u64;
            if !is_allocated(&self.metadata, page_id) {
                return Err(DbError::Storage(format!("page {page_id} is not allocated")));
            }
        }
        for (index, payload) in payloads.iter().enumerate() {
            let page_id = start_page_id + index as u64;
            let encoded = encode_data_page(page_id, self.metadata.generation, payload);
            if let Err(error) = write_at(&mut self.file, page_offset(page_id)?, &encoded) {
                self.poisoned = true;
                return Err(error);
            }
        }
        if let Err(error) = self.file.sync_data() {
            self.poisoned = true;
            return Err(storage_error(error));
        }
        for (index, payload) in payloads.iter().enumerate() {
            self.page_cache.insert(start_page_id + index as u64, payload.clone());
        }
        let _ = end;
        Ok(())
    }

    /// Reads and validates one allocated page, using the bounded LRU cache on hits.
    pub(crate) fn read_page(&mut self, page_id: u64) -> Result<Vec<u8>, DbError> {
        self.ensure_healthy()?;
        if !is_allocated(&self.metadata, page_id) {
            return Err(DbError::Storage(format!("page {page_id} is not allocated")));
        }
        if let Some(payload) = self.page_cache.get(page_id) {
            return Ok(payload);
        }

        let mut encoded = [0u8; PAGE_SIZE];
        self.file.seek(SeekFrom::Start(page_offset(page_id)?)).map_err(storage_error)?;
        self.file.read_exact(&mut encoded).map_err(storage_error)?;
        let payload = decode_data_page(page_id, &encoded, self.metadata.generation)?;
        self.page_cache.insert(page_id, payload.clone());
        Ok(payload)
    }

    /// Returns cache occupancy and cumulative hit/miss/eviction counters.
    pub(crate) fn page_cache_stats(&self) -> PageCacheStats {
        self.page_cache.stats()
    }

    /// Changes the per-handle cache capacity. Shrinking evicts least-recently-used pages.
    pub(crate) fn set_page_cache_capacity(&mut self, capacity_pages: usize) -> Result<(), DbError> {
        self.page_cache.set_capacity(capacity_pages)
    }

    /// Releases a contiguous range. Adjacent free extents are coalesced before
    /// the new metadata generation is made durable.
    pub(crate) fn release_extent(
        &mut self,
        start_page_id: u64,
        page_count: u64,
    ) -> Result<(), DbError> {
        self.ensure_healthy()?;
        if page_count == 0 {
            return Err(DbError::Storage("cannot release an empty extent".into()));
        }
        let end = start_page_id.checked_add(page_count)
            .ok_or_else(|| DbError::Storage("released extent end overflow".into()))?;
        if end > self.metadata.next_page_id {
            return Err(DbError::Storage("released extent exceeds allocator high-water mark".into()));
        }

        let mut next = self.metadata.clone();
        insert_free_extent(&mut next, start_page_id, page_count)?;
        self.commit_metadata(next)?;
        self.page_cache.remove_range(start_page_id, end);
        Ok(())
    }

    /// Lists allocated logical page ranges without expanding free extents.
    /// Callers must prove page-level unreachability before releasing pages.
    pub(crate) fn allocated_extents(&self) -> Vec<(u64, u64)> {
        let mut allocated = Vec::new();
        let mut cursor = 0u64;
        for (&free_start, &free_len) in &self.metadata.free_extents {
            if cursor < free_start { allocated.push((cursor, free_start - cursor)); }
            cursor = free_start.saturating_add(free_len);
        }
        if cursor < self.metadata.next_page_id { allocated.push((cursor, self.metadata.next_page_id - cursor)); }
        allocated
    }

    pub(crate) fn root_page_id(&self) -> Option<u64> {
        self.metadata.root_page_id
    }

    /// Publish a fully-written copy-on-write root. The metadata commit is the
    /// atomic visibility point; callers must sync all referenced pages first.
    pub(crate) fn publish_root(&mut self, root_page_id: Option<u64>) -> Result<(), DbError> {
        self.ensure_healthy()?;
        if let Some(page_id) = root_page_id {
            if !is_allocated(&self.metadata, page_id) {
                return Err(DbError::Storage(format!("root page {page_id} is not allocated")));
            }
            self.read_page(page_id)?;
        }
        let mut next = self.metadata.clone();
        next.root_page_id = root_page_id;
        self.commit_metadata(next)
    }

    pub(crate) fn stats(&self) -> ExtentStats {
        let free_pages = self.metadata.free_extents.values().copied().sum::<u64>();
        ExtentStats {
            next_page_id: self.metadata.next_page_id,
            allocated_pages: self.metadata.next_page_id - free_pages,
            free_pages,
            free_extent_count: self.metadata.free_extents.len(),
        }
    }

    fn commit_metadata(&mut self, mut next: AllocatorMetadata) -> Result<(), DbError> {
        next.generation = self.metadata.generation.checked_add(1)
            .ok_or_else(|| DbError::Storage("extent metadata generation exhausted".into()))?;
        let encoded = encode_metadata(&next)?;
        let first_slot = next.generation % METADATA_SLOTS;
        let second_slot = (first_slot + 1) % METADATA_SLOTS;

        // Persist the new generation to one slot, then mirror it to the other.
        // After this method returns, either metadata slot independently contains
        // the acknowledged generation. If a crash interrupts the first write,
        // the untouched slot still contains the previous generation; if it
        // interrupts the mirror, the first slot already contains the new one.
        if let Err(error) = write_at(&mut self.file, first_slot * PAGE_SIZE as u64, &encoded) {
            self.poisoned = true;
            return Err(error);
        }
        if let Err(error) = self.file.sync_all() {
            self.poisoned = true;
            return Err(storage_error(error));
        }
        // The first durable copy is now the committed generation, even if
        // mirroring the second copy subsequently fails.
        self.metadata = next.clone();

        // A failed return after the first durable copy has an uncertain outcome:
        // the generation is committed even though the caller did not receive
        // success. Poison this handle and require reopen/reconciliation.
        #[cfg(test)]
        if take_metadata_failpoint(MetadataFailpoint::AfterPrimarySync) {
            self.poisoned = true;
            return Err(DbError::Storage(
                "injected failure after primary allocator metadata sync".into(),
            ));
        }

        if let Err(error) = write_at(&mut self.file, second_slot * PAGE_SIZE as u64, &encoded) {
            self.poisoned = true;
            return Err(error);
        }
        if let Err(error) = self.file.sync_all() {
            self.poisoned = true;
            return Err(storage_error(error));
        }

        #[cfg(test)]
        if take_metadata_failpoint(MetadataFailpoint::AfterMirrorSync) {
            self.poisoned = true;
            return Err(DbError::Storage(
                "injected failure after mirrored allocator metadata sync".into(),
            ));
        }

        self.metadata = next;
        Ok(())
    }
}

fn reserve_extent(metadata: &mut AllocatorMetadata, page_count: u64) -> Result<u64, DbError> {
    let reusable = metadata.free_extents
        .iter()
        .find(|(_, len)| **len >= page_count)
        .map(|(start, len)| (*start, *len));

    if let Some((start, available)) = reusable {
        metadata.free_extents.remove(&start);
        if available > page_count {
            metadata.free_extents.insert(start + page_count, available - page_count);
        }
        return Ok(start);
    }

    let start = metadata.next_page_id;
    metadata.next_page_id = metadata.next_page_id.checked_add(page_count)
        .ok_or_else(|| DbError::Storage("page ID space exhausted".into()))?;
    Ok(start)
}

fn insert_free_extent(
    metadata: &mut AllocatorMetadata,
    start: u64,
    count: u64,
) -> Result<(), DbError> {
    let end = start.checked_add(count)
        .ok_or_else(|| DbError::Storage("free extent end overflow".into()))?;

    let previous = metadata.free_extents.range(..=start).next_back()
        .map(|(prev_start, prev_len)| (*prev_start, *prev_len));
    if let Some((prev_start, prev_len)) = previous {
        let prev_end = prev_start.checked_add(prev_len)
            .ok_or_else(|| DbError::Storage("existing free extent overflow".into()))?;
        if prev_end > start {
            return Err(DbError::Storage("page extent is already free".into()));
        }
    }

    let following = metadata.free_extents.range(start..).next()
        .map(|(next_start, next_len)| (*next_start, *next_len));
    if let Some((next_start, _)) = following {
        if next_start < end {
            return Err(DbError::Storage("released extent overlaps free pages".into()));
        }
    }

    let mut merged_start = start;
    let mut merged_end = end;
    if let Some((prev_start, prev_len)) = previous {
        if prev_start + prev_len == start {
            metadata.free_extents.remove(&prev_start);
            merged_start = prev_start;
        }
    }
    if let Some((next_start, next_len)) = following {
        if next_start == end {
            metadata.free_extents.remove(&next_start);
            merged_end = next_start.checked_add(next_len)
                .ok_or_else(|| DbError::Storage("merged free extent overflow".into()))?;
        }
    }
    metadata.free_extents.insert(merged_start, merged_end - merged_start);
    Ok(())
}

fn is_allocated(metadata: &AllocatorMetadata, page_id: u64) -> bool {
    if page_id >= metadata.next_page_id {
        return false;
    }
    match metadata.free_extents.range(..=page_id).next_back() {
        Some((start, len)) => page_id >= start.saturating_add(*len),
        None => true,
    }
}

fn validate_metadata(metadata: &AllocatorMetadata) -> Result<(), DbError> {
    if metadata.root_page_id.is_some_and(|root| root >= metadata.next_page_id || !is_allocated(metadata, root)) {
        return Err(DbError::Storage("metadata root points to an unallocated page".into()));
    }
    let mut previous_end = None;
    for (start, count) in &metadata.free_extents {
        if *count == 0 {
            return Err(DbError::Storage("metadata contains an empty free extent".into()));
        }
        let end = start.checked_add(*count)
            .ok_or_else(|| DbError::Storage("metadata free extent overflow".into()))?;
        if end > metadata.next_page_id {
            return Err(DbError::Storage("metadata free extent exceeds high-water mark".into()));
        }
        if previous_end.is_some_and(|previous| previous >= *start) {
            return Err(DbError::Storage("metadata free extents overlap or are not coalesced".into()));
        }
        previous_end = Some(end);
    }
    Ok(())
}

fn encode_metadata(metadata: &AllocatorMetadata) -> Result<[u8; PAGE_SIZE], DbError> {
    validate_metadata(metadata)?;
    if metadata.free_extents.len() > MAX_FREE_EXTENTS {
        return Err(DbError::Storage("free extent metadata exceeds one-page capacity".into()));
    }

    let mut page = [0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(METADATA_MAGIC);
    page[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    page[8..16].copy_from_slice(&metadata.generation.to_le_bytes());
    page[16..24].copy_from_slice(&metadata.next_page_id.to_le_bytes());
    page[24..28].copy_from_slice(&(metadata.free_extents.len() as u32).to_le_bytes());
    page[28..36].copy_from_slice(&metadata.root_page_id.unwrap_or(u64::MAX).to_le_bytes());

    let mut offset = METADATA_HEADER_SIZE;
    for (start, count) in &metadata.free_extents {
        page[offset..offset + 8].copy_from_slice(&start.to_le_bytes());
        page[offset + 8..offset + 16].copy_from_slice(&count.to_le_bytes());
        offset += EXTENT_ENTRY_SIZE;
    }
    let checksum = super::checksum(&page[..offset]);
    page[36..44].copy_from_slice(&checksum.to_le_bytes());
    Ok(page)
}

fn decode_metadata(page: &[u8; PAGE_SIZE]) -> Result<AllocatorMetadata, DbError> {
    let version = u32::from_le_bytes(page[4..8].try_into().unwrap());
    let valid_magic = (&page[0..4] == b"EAM2" && version == 2)
        || (&page[0..4] == METADATA_MAGIC && version == FORMAT_VERSION);
    if !valid_magic {
        return Err(DbError::Storage("invalid extent metadata magic or version".into()));
    }

    let generation = u64::from_le_bytes(page[8..16].try_into().unwrap());
    let next_page_id = u64::from_le_bytes(page[16..24].try_into().unwrap());
    let extent_count = u32::from_le_bytes(page[24..28].try_into().unwrap()) as usize;
    let (header_size, checksum_offset, root_page_id) = if version == 2 {
        (36usize, 28usize, None)
    } else {
        let root = u64::from_le_bytes(page[28..36].try_into().unwrap());
        (44usize, 36usize, (root != u64::MAX).then_some(root))
    };
    let max_extents = (PAGE_SIZE - header_size) / EXTENT_ENTRY_SIZE;
    if extent_count > max_extents {
        return Err(DbError::Storage("extent metadata entry count exceeds page capacity".into()));
    }
    let used = header_size + extent_count * EXTENT_ENTRY_SIZE;
    let expected_checksum = u64::from_le_bytes(page[checksum_offset..checksum_offset + 8].try_into().unwrap());
    let mut checksum_bytes = page[..used].to_vec();
    checksum_bytes[checksum_offset..checksum_offset + 8].fill(0);
    if super::checksum(&checksum_bytes) != expected_checksum {
        return Err(DbError::Storage("extent metadata checksum mismatch".into()));
    }
    if page[used..].iter().any(|byte| *byte != 0) {
        return Err(DbError::Storage("non-zero extent metadata padding".into()));
    }

    let mut free_extents = BTreeMap::new();
    let mut offset = header_size;
    for _ in 0..extent_count {
        let start = u64::from_le_bytes(page[offset..offset + 8].try_into().unwrap());
        let count = u64::from_le_bytes(page[offset + 8..offset + 16].try_into().unwrap());
        if free_extents.insert(start, count).is_some() {
            return Err(DbError::Storage("duplicate free extent start in metadata".into()));
        }
        offset += EXTENT_ENTRY_SIZE;
    }

    let metadata = AllocatorMetadata { generation, next_page_id, root_page_id, free_extents };
    validate_metadata(&metadata)?;
    Ok(metadata)
}

fn encode_data_page(
    page_id: u64,
    generation: u64,
    payload: &[u8],
) -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(DATA_PAGE_MAGIC);
    page[4..12].copy_from_slice(&page_id.to_le_bytes());
    page[12..20].copy_from_slice(&generation.to_le_bytes());
    page[20..24].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    page[24..32].copy_from_slice(&data_page_checksum(page_id, generation, payload).to_le_bytes());
    page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + payload.len()].copy_from_slice(payload);
    page
}

fn data_page_checksum(page_id: u64, generation: u64, payload: &[u8]) -> u64 {
    let mut bytes = Vec::with_capacity(20 + payload.len());
    bytes.extend_from_slice(&page_id.to_le_bytes());
    bytes.extend_from_slice(&generation.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(payload);
    super::checksum(&bytes)
}

fn decode_data_page(
    page_id: u64,
    page: &[u8; PAGE_SIZE],
    max_generation: u64,
) -> Result<Vec<u8>, DbError> {
    if &page[0..4] != DATA_PAGE_MAGIC {
        return Err(DbError::Storage(format!("invalid data-page magic for page {page_id}")));
    }
    let stored_page_id = u64::from_le_bytes(page[4..12].try_into().unwrap());
    if stored_page_id != page_id {
        return Err(DbError::Storage(format!("data-page ID mismatch for page {page_id}")));
    }
    let generation = u64::from_le_bytes(page[12..20].try_into().unwrap());
    if generation == 0 || generation > max_generation {
        return Err(DbError::Storage(format!("invalid generation for page {page_id}")));
    }
    let used = u32::from_le_bytes(page[20..24].try_into().unwrap()) as usize;
    if used > PAGE_PAYLOAD_SIZE {
        return Err(DbError::Storage(format!("invalid payload length for page {page_id}")));
    }
    let payload = &page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + used];
    let expected_checksum = u64::from_le_bytes(page[24..32].try_into().unwrap());
    if data_page_checksum(page_id, generation, payload) != expected_checksum {
        return Err(DbError::Storage(format!("data-page checksum mismatch for page {page_id}")));
    }
    if page[PAGE_HEADER_SIZE + used..].iter().any(|byte| *byte != 0) {
        return Err(DbError::Storage(format!("non-zero data-page padding for page {page_id}")));
    }
    Ok(payload.to_vec())
}

fn read_metadata_slot(file: &mut File, slot: u64) -> Result<Option<AllocatorMetadata>, DbError> {
    let mut page = [0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(slot * PAGE_SIZE as u64)).map_err(storage_error)?;
    file.read_exact(&mut page).map_err(storage_error)?;
    if page.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    decode_metadata(&page).map(Some)
}

fn write_at(file: &mut File, offset: u64, bytes: &[u8]) -> Result<(), DbError> {
    file.seek(SeekFrom::Start(offset)).map_err(storage_error)?;
    file.write_all(bytes).map_err(storage_error)
}

fn page_offset(page_id: u64) -> Result<u64, DbError> {
    let page_offset = page_id.checked_mul(PAGE_SIZE as u64)
        .and_then(|offset| METADATA_BYTES.checked_add(offset))
        .ok_or_else(|| DbError::Storage("page-file offset overflow".into()))?;
    Ok(page_offset)
}

fn storage_error(error: std::io::Error) -> DbError {
    DbError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{decode_metadata, ExtentFile, PAGE_SIZE};
    use std::fs::{self, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_path() -> PathBuf {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "postemberdb-extent-{}-{id}.pdb",
            std::process::id()
        ))
    }

    #[test]
    fn page_contents_and_allocator_metadata_survive_reopen() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        let page_id = store.allocate_page(b"ember-tree-page").unwrap();
        assert_eq!(page_id, 0);
        assert_eq!(store.read_page(page_id).unwrap(), b"ember-tree-page");
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.read_page(page_id).unwrap(), b"ember-tree-page");
        assert_eq!(reopened.stats().allocated_pages, 1);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn freed_ranges_coalesce_and_are_reused_after_reopen() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        let first = store.allocate_page(b"a").unwrap();
        let second = store.allocate_page(b"b").unwrap();
        let third = store.allocate_page(b"c").unwrap();
        assert_eq!((first, second, third), (0, 1, 2));

        store.release_extent(second, 1).unwrap();
        assert_eq!(store.allocate_page(b"new-b").unwrap(), second);
        store.release_extent(second, 1).unwrap();
        store.release_extent(first, 1).unwrap();
        store.release_extent(third, 1).unwrap();

        assert_eq!(store.stats().free_pages, 3);
        assert_eq!(store.stats().free_extent_count, 1);
        assert_eq!(store.allocate_page(b"reused").unwrap(), 0);
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.read_page(0).unwrap(), b"reused");
        assert_eq!(reopened.stats().allocated_pages, 1);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn decodes_legacy_eam2_metadata_without_a_tree_root() {
        let mut page = [0u8; PAGE_SIZE];
        page[0..4].copy_from_slice(b"EAM2");
        page[4..8].copy_from_slice(&2u32.to_le_bytes());
        page[8..16].copy_from_slice(&7u64.to_le_bytes());
        page[16..24].copy_from_slice(&0u64.to_le_bytes());
        page[24..28].copy_from_slice(&0u32.to_le_bytes());
        let checksum = super::super::checksum(&page[..36]);
        page[28..36].copy_from_slice(&checksum.to_le_bytes());

        let decoded = decode_metadata(&page).unwrap();
        assert_eq!(decoded.generation, 7);
        assert_eq!(decoded.root_page_id, None);
        assert!(decoded.free_extents.is_empty());
    }

    #[test]
    fn reserves_contiguous_extents_and_persists_written_pages() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        let start = store.allocate_extent(3).unwrap();
        assert_eq!(start, 0);
        store.write_page(start, b"first").unwrap();
        store.write_page(start + 1, b"second").unwrap();
        store.write_page(start + 2, b"third").unwrap();
        assert_eq!(store.read_page(start + 1).unwrap(), b"second");
        assert_eq!(store.stats().allocated_pages, 3);
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.read_page(start).unwrap(), b"first");
        assert_eq!(reopened.read_page(start + 2).unwrap(), b"third");
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn allocator_rejects_double_release_and_out_of_range_extents() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        store.allocate_page(b"page").unwrap();

        assert!(store.release_extent(0, 2).is_err());
        store.release_extent(0, 1).unwrap();
        assert!(store.release_extent(0, 1).is_err());
        assert!(store.read_page(0).is_err());
        drop(store);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn redundant_metadata_slot_recovers_latest_generation() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        let page_id = store.allocate_page(b"durably-allocated").unwrap();
        drop(store);

        // A successful metadata commit mirrors its generation to both slots.
        // Corrupting either copy must not roll back the acknowledged allocation.
        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"bad!").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.read_page(page_id).unwrap(), b"durably-allocated");
        assert_eq!(reopened.stats().allocated_pages, 1);
        let repaired = fs::read(&path).unwrap();
        assert_eq!(&repaired[..PAGE_SIZE], &repaired[PAGE_SIZE..2 * PAGE_SIZE]);
        let next_page = reopened.allocate_page(b"after-recovery").unwrap();
        assert_eq!(next_page, 1);
        assert_eq!(reopened.read_page(next_page).unwrap(), b"after-recovery");
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn allocator_reservation_survives_failure_after_primary_metadata_sync() {
        use super::{set_metadata_failpoint, MetadataFailpoint};

        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        set_metadata_failpoint(Some(MetadataFailpoint::AfterPrimarySync));
        let error = store.allocate_extent(3).unwrap_err();
        assert!(error.to_string().contains("primary allocator metadata sync"));
        assert!(store.allocate_extent(1).is_err());
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.stats().next_page_id, 3);
        assert_eq!(reopened.stats().allocated_pages, 3);
        // The failed reservation is intentionally leaked, never reissued.
        assert_eq!(reopened.allocate_extent(1).unwrap(), 3);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn mirrored_metadata_failure_reopens_with_the_committed_generation() {
        use super::{set_metadata_failpoint, MetadataFailpoint};

        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        set_metadata_failpoint(Some(MetadataFailpoint::AfterMirrorSync));
        let error = store.allocate_extent(2).unwrap_err();
        assert!(error.to_string().contains("mirrored allocator metadata sync"));
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert_eq!(reopened.stats().next_page_id, 2);
        assert_eq!(reopened.stats().allocated_pages, 2);
        assert_eq!(reopened.allocate_extent(1).unwrap(), 2);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn maximum_page_payload_round_trips() {
        let path = temp_path();
        let payload = vec![0xA5; super::PAGE_PAYLOAD_SIZE];
        let mut store = ExtentFile::open(&path).unwrap();
        let page_id = store.allocate_page(&payload).unwrap();
        assert_eq!(store.read_page(page_id).unwrap(), payload);
        drop(store);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn both_corrupt_metadata_copies_fail_closed() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        store.allocate_page(b"committed").unwrap();
        drop(store);

        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        for slot in 0..2u64 {
            file.seek(SeekFrom::Start(slot * PAGE_SIZE as u64)).unwrap();
            file.write_all(b"bad!").unwrap();
        }
        file.sync_all().unwrap();
        drop(file);

        assert!(ExtentFile::open(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn page_cache_serves_hits_and_evicts_least_recently_used_pages() {
        let path = temp_path();
        let mut store = ExtentFile::open_with_cache_capacity(&path, 0).unwrap();
        let first = store.allocate_page(b"first").unwrap();
        let second = store.allocate_page(b"second").unwrap();
        let third = store.allocate_page(b"third").unwrap();

        store.set_page_cache_capacity(2).unwrap();
        assert_eq!(store.read_page(first).unwrap(), b"first");
        assert_eq!(store.read_page(first).unwrap(), b"first");
        assert_eq!(store.read_page(second).unwrap(), b"second");
        assert_eq!(store.read_page(first).unwrap(), b"first");
        assert_eq!(store.read_page(third).unwrap(), b"third");
        assert_eq!(store.read_page(second).unwrap(), b"second");

        let stats = store.page_cache_stats();
        assert_eq!(stats.capacity_pages, 2);
        assert_eq!(stats.resident_pages, 2);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 4);
        assert_eq!(stats.evictions, 2);
        drop(store);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn page_cache_is_updated_after_writes_and_invalidated_when_pages_are_released() {
        let path = temp_path();
        let mut store = ExtentFile::open_with_cache_capacity(&path, 2).unwrap();
        let page_id = store.allocate_page(b"before").unwrap();
        assert_eq!(store.read_page(page_id).unwrap(), b"before");
        store.write_page(page_id, b"after").unwrap();
        assert_eq!(store.read_page(page_id).unwrap(), b"after");

        store.release_extent(page_id, 1).unwrap();
        assert_eq!(store.page_cache_stats().resident_pages, 0);
        let reused = store.allocate_page(b"reused").unwrap();
        assert_eq!(reused, page_id);
        assert_eq!(store.read_page(reused).unwrap(), b"reused");
        drop(store);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn page_cache_capacity_is_bounded_and_zero_disables_residency() {
        let path = temp_path();
        assert!(ExtentFile::open_with_cache_capacity(&path, 65_537).is_err());

        let mut store = ExtentFile::open_with_cache_capacity(&path, 0).unwrap();
        let page_id = store.allocate_page(b"uncached").unwrap();
        assert_eq!(store.read_page(page_id).unwrap(), b"uncached");
        assert_eq!(store.read_page(page_id).unwrap(), b"uncached");
        let stats = store.page_cache_stats();
        assert_eq!(stats.capacity_pages, 0);
        assert_eq!(stats.resident_pages, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 2);
        drop(store);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn checksums_reject_corrupt_page_payloads() {
        let path = temp_path();
        let mut store = ExtentFile::open(&path).unwrap();
        let page_id = store.allocate_page(b"integrity").unwrap();
        drop(store);

        let data_offset = (2 * PAGE_SIZE) as u64 + page_id * PAGE_SIZE as u64 + 32;
        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(data_offset)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reopened = ExtentFile::open(&path).unwrap();
        assert!(reopened.read_page(page_id).unwrap_err().to_string().contains("checksum"));
        drop(reopened);
        fs::remove_file(path).unwrap();
    }
}
