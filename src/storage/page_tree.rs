//! Copy-on-write, page-backed B+ tree snapshots.
//!
//! Each replacement builds immutable leaf/internal pages, writes a redo record,
//! syncs the pages, then publishes the root through the extent metadata. Old
//! pages are intentionally retained until a later reachability-aware vacuum.

use super::extent::ExtentFile;
use super::DbError;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

const LEAF_MAGIC: &[u8; 4] = b"EBL1";
const INTERNAL_MAGIC: &[u8; 4] = b"EBI1";
const WAL_MAGIC: &[u8; 4] = b"PBTW";
const WAL_HEADER_SIZE: usize = 28;
const MAX_WAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 1024;

#[derive(Debug, Clone)]
struct NodeRef {
    page_id: u64,
    first_key: Vec<u8>,
}

enum DecodedNode {
    Leaf(Vec<(Vec<u8>, Vec<u8>)>),
    Internal(Vec<(Vec<u8>, u64)>),
}

/// Rebuilds a sorted key/value tree using immutable pages and atomically swaps
/// its root. Keys must be unique and sorted in ascending byte order.
pub(crate) fn replace_all(
    store: &mut ExtentFile,
    wal_path: &Path,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Result<(), DbError> {
    recover(store, wal_path)?;
    validate_entries(entries)?;
    let mut pages: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut level = Vec::new();

    let groups = group_leaf_entries(entries)?;
    for group in groups {
        let page_id = store.allocate_extent(1)?;
        let payload = encode_leaf(&group)?;
        let first_key = group.first().map(|entry| entry.0.clone()).unwrap_or_default();
        pages.push((page_id, payload));
        level.push(NodeRef { page_id, first_key });
    }

    while level.len() > 1 {
        let groups = group_internal_children(&level)?;
        let mut next_level = Vec::with_capacity(groups.len());
        for group in groups {
            let page_id = store.allocate_extent(1)?;
            let payload = encode_internal(&group)?;
            next_level.push(NodeRef {
                page_id,
                first_key: group[0].first_key.clone(),
            });
            pages.push((page_id, payload));
        }
        level = next_level;
    }

    let root_page_id = level[0].page_id;
    let wal = encode_wal(root_page_id, &pages)?;
    write_wal(wal_path, &wal)?;

    for (page_id, payload) in &pages {
        store.write_page(*page_id, payload)?;
    }
    store.publish_root(Some(root_page_id))?;
    truncate_wal(wal_path)?;
    Ok(())
}

/// Replays a complete pending tree redo record before the tree is made visible.
pub(crate) fn recover(store: &mut ExtentFile, wal_path: &Path) -> Result<(), DbError> {
    if !wal_path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new().read(true).write(true).open(wal_path)
        .map_err(storage_error)?;
    let mut bytes = Vec::new();
    let mut limited = (&mut file).take((MAX_WAL_BYTES + 1) as u64);
    limited.read_to_end(&mut bytes).map_err(storage_error)?;
    drop(limited);
    if bytes.is_empty() {
        return Ok(());
    }
    if bytes.len() > MAX_WAL_BYTES {
        return Err(DbError::Storage("B+ tree WAL exceeds 64 MiB".into()));
    }
    if bytes.len() < WAL_HEADER_SIZE {
        file.set_len(0).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        return Ok(());
    }

    let (root_page_id, pages) = match decode_wal(&bytes) {
        Ok(decoded) => decoded,
        Err(_error) if is_incomplete_wal(&bytes) => {
            file.set_len(0).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    for (page_id, payload) in pages {
        store.write_page(page_id, &payload)?;
    }
    store.publish_root(Some(root_page_id))?;
    file.set_len(0).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)?;
    Ok(())
}

pub(crate) fn get(
    store: &mut ExtentFile,
    key: &[u8],
) -> Result<Option<Vec<u8>>, DbError> {
    let Some(mut page_id) = store.root_page_id() else { return Ok(None); };
    loop {
        match decode_node(&store.read_page(page_id)?)? {
            DecodedNode::Leaf(entries) => {
                return Ok(entries.binary_search_by(|(candidate, _)| candidate.as_slice().cmp(key))
                    .ok().map(|index| entries[index].1.clone()));
            }
            DecodedNode::Internal(children) => {
                let child_index = children[1..].partition_point(|(separator, _)| key >= separator.as_slice());
                page_id = children[child_index].1;
            }
        }
    }
}

pub(crate) fn scan_range(
    store: &mut ExtentFile,
    lower: &[u8],
    upper: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
    scan_range_limited(store, lower, upper, None)
}

/// Scans an inclusive key range and stops as soon as the requested number of
/// entries has been collected. Internal separators prune subtrees outside the
/// upper bound, so LIMIT queries do not traverse the remainder of the tree.
pub(crate) fn scan_range_limited(
    store: &mut ExtentFile,
    lower: &[u8],
    upper: &[u8],
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DbError> {
    let mut out = Vec::new();
    if lower > upper || limit == Some(0) { return Ok(out); }
    let Some(root) = store.root_page_id() else { return Ok(out); };
    scan_node_limited(store, root, lower, upper, limit, &mut out)?;
    Ok(out)
}

fn scan_node_limited(
    store: &mut ExtentFile,
    page_id: u64,
    lower: &[u8],
    upper: &[u8],
    limit: Option<usize>,
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), DbError> {
    if limit.is_some_and(|maximum| out.len() >= maximum) { return Ok(()); }
    match decode_node(&store.read_page(page_id)?)? {
        DecodedNode::Leaf(entries) => {
            let start = entries.partition_point(|(key, _)| key.as_slice() < lower);
            let end = entries.partition_point(|(key, _)| key.as_slice() <= upper);
            for (key, value) in entries.into_iter().take(end).skip(start) {
                out.push((key, value));
                if limit.is_some_and(|maximum| out.len() >= maximum) { break; }
            }
        }
        DecodedNode::Internal(children) => {
            let first = children[1..].partition_point(|(separator, _)| separator.as_slice() <= lower);
            for (first_key, child) in children.into_iter().skip(first) {
                if first_key.as_slice() > upper { break; }
                scan_node_limited(store, child, lower, upper, limit, out)?;
                if limit.is_some_and(|maximum| out.len() >= maximum) { break; }
            }
        }
    }
    Ok(())
}

/// Validates the complete reachable tree, including child ranges and separator
/// equality. It also rejects cycles and pages referenced by multiple parents.
pub(crate) fn validate_tree(store: &mut ExtentFile) -> Result<(), DbError> {
    let Some(root) = store.root_page_id() else { return Ok(()); };
    let allocation_bound = store.stats().allocated_pages;
    let mut visited = HashSet::new();
    validate_subtree(store, root, &mut visited, allocation_bound)?;
    Ok(())
}

fn validate_subtree(
    store: &mut ExtentFile,
    page_id: u64,
    visited: &mut HashSet<u64>,
    allocation_bound: u64,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, DbError> {
    if !visited.insert(page_id) {
        return Err(DbError::Storage("B+ tree contains a cycle or multiply-referenced page".into()));
    }
    if visited.len() as u64 > allocation_bound {
        return Err(DbError::Storage("B+ tree reachability exceeds allocated page count".into()));
    }
    match decode_node(&store.read_page(page_id)?)? {
        DecodedNode::Leaf(entries) => {
            Ok(entries.first().zip(entries.last()).map(|(first, last)| (first.0.clone(), last.0.clone())))
        }
        DecodedNode::Internal(children) => {
            if children.len() < 2 {
                return Err(DbError::Storage("B+ tree internal node has fewer than two children".into()));
            }
            let mut previous_max: Option<Vec<u8>> = None;
            let mut overall_min: Option<Vec<u8>> = None;
            let mut overall_max: Option<Vec<u8>> = None;
            for (index, (separator, child_id)) in children.iter().enumerate() {
                let range = validate_subtree(store, *child_id, visited, allocation_bound)?
                    .ok_or_else(|| DbError::Storage("B+ tree internal node references an empty child".into()))?;
                if index == 0 {
                    overall_min = Some(range.0.clone());
                } else {
                    if separator != &range.0 {
                        return Err(DbError::Storage("B+ tree separator does not match right-child minimum".into()));
                    }
                    if previous_max.as_ref().is_some_and(|maximum| maximum >= &range.0) {
                        return Err(DbError::Storage("B+ tree child key ranges overlap or are unordered".into()));
                    }
                }
                previous_max = Some(range.1.clone());
                overall_max = Some(range.1);
            }
            Ok(overall_min.zip(overall_max))
        }
    }
}

fn validate_entries(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
    for (index, (key, value)) in entries.iter().enumerate() {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(DbError::Storage("B+ tree keys must contain 1..=1024 bytes".into()));
        }
        if value.len() > super::extent::page_payload_size() {
            return Err(DbError::Storage("B+ tree value exceeds one page".into()));
        }
        if index > 0 && entries[index - 1].0.as_slice() >= key.as_slice() {
            return Err(DbError::Storage("B+ tree keys must be strictly increasing".into()));
        }
    }
    Ok(())
}

fn group_leaf_entries(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<Vec<(Vec<u8>, Vec<u8>)>>, DbError> {
    if entries.is_empty() { return Ok(vec![Vec::new()]); }
    let mut groups: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
    let mut current = Vec::new();
    let mut used = 6usize;
    for entry in entries {
        let size = 4usize.checked_add(entry.0.len()).and_then(|n| n.checked_add(entry.1.len()))
            .ok_or_else(|| DbError::Storage("B+ tree leaf size overflow".into()))?;
        if size + 6 > super::extent::page_payload_size() {
            return Err(DbError::Storage("B+ tree entry does not fit in a page".into()));
        }
        if !current.is_empty() && used + size > super::extent::page_payload_size() {
            groups.push(std::mem::take(&mut current));
            used = 6;
        }
        used += size;
        current.push(entry.clone());
    }
    if !current.is_empty() { groups.push(current); }
    Ok(groups)
}

fn group_internal_children(children: &[NodeRef]) -> Result<Vec<Vec<NodeRef>>, DbError> {
    let mut groups: Vec<Vec<NodeRef>> = Vec::new();
    let mut current = Vec::new();
    let mut used = 14usize;
    for child in children {
        let size = if current.is_empty() { 0 } else { 10 + child.first_key.len() };
        if !current.is_empty() && used + size > super::extent::page_payload_size() {
            groups.push(std::mem::take(&mut current));
            used = 14;
        }
        current.push(child.clone());
        used += size;
    }
    if !current.is_empty() { groups.push(current); }
    if groups.len() > 1 && groups.last().is_some_and(|group| group.len() == 1) {
        let last = groups.pop().unwrap().remove(0);
        let previous = groups.last_mut().unwrap();
        if previous.len() <= 2 {
            return Err(DbError::Storage("cannot balance B+ tree internal page groups".into()));
        }
        let moved = previous.pop().unwrap();
        groups.push(vec![moved, last]);
    }
    Ok(groups)
}

fn encode_leaf(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<u8>, DbError> {
    let mut out = Vec::new();
    out.extend_from_slice(LEAF_MAGIC);
    put_u16(&mut out, entries.len())?;
    for (key, value) in entries {
        put_u16(&mut out, key.len())?;
        put_u16(&mut out, value.len())?;
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }
    if out.len() > super::extent::page_payload_size() {
        return Err(DbError::Storage("encoded B+ tree leaf exceeds page size".into()));
    }
    Ok(out)
}

fn encode_internal(children: &[NodeRef]) -> Result<Vec<u8>, DbError> {
    if children.len() < 2 {
        return Err(DbError::Storage("B+ tree internal node requires at least two children".into()));
    }
    let mut out = Vec::new();
    out.extend_from_slice(INTERNAL_MAGIC);
    put_u16(&mut out, children.len() - 1)?;
    out.extend_from_slice(&children[0].page_id.to_le_bytes());
    for child in children.iter().skip(1) {
        put_u16(&mut out, child.first_key.len())?;
        out.extend_from_slice(&child.first_key);
        out.extend_from_slice(&child.page_id.to_le_bytes());
    }
    if out.len() > super::extent::page_payload_size() {
        return Err(DbError::Storage("encoded B+ tree internal node exceeds page size".into()));
    }
    Ok(out)
}

fn decode_node(bytes: &[u8]) -> Result<DecodedNode, DbError> {
    if bytes.len() < 6 {
        return Err(DbError::Storage("truncated B+ tree node".into()));
    }
    if &bytes[..4] == LEAF_MAGIC {
        let count = get_u16(bytes, 4)? as usize;
        let mut offset = 6usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let key_len = get_u16(bytes, offset)? as usize; offset += 2;
            let value_len = get_u16(bytes, offset)? as usize; offset += 2;
            let key_end = offset.checked_add(key_len).ok_or_else(|| DbError::Storage("leaf key offset overflow".into()))?;
            let value_end = key_end.checked_add(value_len).ok_or_else(|| DbError::Storage("leaf value offset overflow".into()))?;
            if value_end > bytes.len() { return Err(DbError::Storage("truncated B+ tree leaf entry".into())); }
            entries.push((bytes[offset..key_end].to_vec(), bytes[key_end..value_end].to_vec()));
            offset = value_end;
        }
        if offset != bytes.len() { return Err(DbError::Storage("trailing bytes in B+ tree leaf".into())); }
        if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(DbError::Storage("B+ tree leaf keys are not strictly ordered".into()));
        }
        Ok(DecodedNode::Leaf(entries))
    } else if &bytes[..4] == INTERNAL_MAGIC {
        let count = get_u16(bytes, 4)? as usize;
        if count == 0 { return Err(DbError::Storage("empty B+ tree internal node".into())); }
        if bytes.len() < 14 { return Err(DbError::Storage("truncated B+ tree internal node".into())); }
        let first_child = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
        let mut children = vec![(Vec::new(), first_child)];
        let mut offset = 14usize;
        for _ in 0..count {
            let key_len = get_u16(bytes, offset)? as usize; offset += 2;
            let key_end = offset.checked_add(key_len).ok_or_else(|| DbError::Storage("internal key offset overflow".into()))?;
            let child_end = key_end.checked_add(8).ok_or_else(|| DbError::Storage("internal child offset overflow".into()))?;
            if child_end > bytes.len() { return Err(DbError::Storage("truncated B+ tree internal entry".into())); }
            let key = bytes[offset..key_end].to_vec();
            let child = u64::from_le_bytes(bytes[key_end..child_end].try_into().unwrap());
            children.push((key, child));
            offset = child_end;
        }
        if offset != bytes.len() { return Err(DbError::Storage("trailing bytes in B+ tree internal node".into())); }
        if children.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(DbError::Storage("B+ tree internal separators are not ordered".into()));
        }
        Ok(DecodedNode::Internal(children))
    } else {
        Err(DbError::Storage("invalid B+ tree node magic".into()))
    }
}

fn encode_wal(root: u64, pages: &[(u64, Vec<u8>)]) -> Result<Vec<u8>, DbError> {
    let mut payload = Vec::new();
    for (page_id, bytes) in pages {
        payload.extend_from_slice(&page_id.to_le_bytes());
        put_u32(&mut payload, bytes.len())?;
        payload.extend_from_slice(bytes);
    }
    if payload.len() > MAX_WAL_BYTES - WAL_HEADER_SIZE {
        return Err(DbError::Storage("B+ tree WAL exceeds 64 MiB".into()));
    }
    let mut out = Vec::with_capacity(WAL_HEADER_SIZE + payload.len());
    out.extend_from_slice(WAL_MAGIC);
    out.extend_from_slice(&root.to_le_bytes());
    put_u32(&mut out, pages.len())?;
    put_u32(&mut out, payload.len())?;
    out.extend_from_slice(&super::checksum(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_wal(bytes: &[u8]) -> Result<(u64, Vec<(u64, Vec<u8>)>), DbError> {
    if bytes.len() < WAL_HEADER_SIZE || &bytes[..4] != WAL_MAGIC {
        return Err(DbError::Storage("invalid B+ tree WAL header".into()));
    }
    let root = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
    let count = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let payload_len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let expected = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
    if payload_len != bytes.len() - WAL_HEADER_SIZE {
        return Err(DbError::Storage("B+ tree WAL payload length mismatch".into()));
    }
    let payload = &bytes[WAL_HEADER_SIZE..];
    if super::checksum(payload) != expected {
        return Err(DbError::Storage("B+ tree WAL checksum mismatch".into()));
    }
    if count > payload.len() / 12 {
        return Err(DbError::Storage("B+ tree WAL page count exceeds payload bounds".into()));
    }
    let mut offset = 0usize;
    let mut pages = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 12 > payload.len() { return Err(DbError::Storage("truncated B+ tree WAL page record".into())); }
        let page_id = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        let len = u32::from_le_bytes(payload[offset + 8..offset + 12].try_into().unwrap()) as usize;
        offset += 12;
        let end = offset.checked_add(len).ok_or_else(|| DbError::Storage("B+ tree WAL page length overflow".into()))?;
        if end > payload.len() { return Err(DbError::Storage("truncated B+ tree WAL page payload".into())); }
        pages.push((page_id, payload[offset..end].to_vec()));
        offset = end;
    }
    if offset != payload.len() { return Err(DbError::Storage("trailing bytes in B+ tree WAL".into())); }
    if pages.is_empty() || !pages.iter().any(|(page_id, _)| *page_id == root) {
        return Err(DbError::Storage("B+ tree WAL root is not present in its page set".into()));
    }
    Ok((root, pages))
}

fn is_incomplete_wal(bytes: &[u8]) -> bool {
    if bytes.len() < WAL_HEADER_SIZE { return true; }
    if &bytes[..4] != WAL_MAGIC { return false; }
    let expected = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    expected > bytes.len() - WAL_HEADER_SIZE
}

fn write_wal(path: &Path, bytes: &[u8]) -> Result<(), DbError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() { fs::create_dir_all(parent).map_err(storage_error)?; }
    }
    let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(path)
        .map_err(storage_error)?;
    file.write_all(bytes).map_err(storage_error)?;
    file.sync_all().map_err(storage_error)
}

fn truncate_wal(path: &Path) -> Result<(), DbError> {
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(path)
        .map_err(storage_error)?;
    file.sync_all().map_err(storage_error)
}

fn storage_error(error: std::io::Error) -> DbError { DbError::Storage(error.to_string()) }

fn put_u16(out: &mut Vec<u8>, value: usize) -> Result<(), DbError> {
    let value = u16::try_from(value).map_err(|_| DbError::Storage("B+ tree field exceeds 65535 bytes".into()))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}
fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), DbError> {
    let value = u32::try_from(value).map_err(|_| DbError::Storage("B+ tree field exceeds 4 GiB".into()))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}
fn get_u16(bytes: &[u8], offset: usize) -> Result<u16, DbError> {
    let end = offset.checked_add(2).ok_or_else(|| DbError::Storage("B+ tree offset overflow".into()))?;
    if end > bytes.len() { return Err(DbError::Storage("truncated B+ tree node field".into())); }
    Ok(u16::from_le_bytes(bytes[offset..end].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn paths(label: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let base = std::env::temp_dir().join(format!("postemberdb-page-tree-{label}-{}-{nonce}.pbd", std::process::id()));
        let mut wal = base.as_os_str().to_os_string();
        wal.push(".btree.wal");
        (base, wal.into())
    }

    #[test]
    fn cow_tree_round_trips_and_publishes_new_root() {
        let (path, wal) = paths("roundtrip");
        let entries = (0..800).map(|n| (format!("key-{n:04}").into_bytes(), format!("value-{n}").into_bytes())).collect::<Vec<_>>();
        let mut store = ExtentFile::open(&path).unwrap();
        replace_all(&mut store, &wal, &entries).unwrap();
        let first_root = store.root_page_id().unwrap();
        assert_eq!(get(&mut store, b"key-0400").unwrap(), Some(b"value-400".to_vec()));
        assert_eq!(scan_range(&mut store, b"key-0010", b"key-0015").unwrap().len(), 6);

        let changed = vec![
            (b"alpha".to_vec(), b"one".to_vec()),
            (b"beta".to_vec(), b"two".to_vec()),
            (b"gamma".to_vec(), b"three".to_vec()),
        ];
        replace_all(&mut store, &wal, &changed).unwrap();
        let second_root = store.root_page_id().unwrap();
        assert_ne!(first_root, second_root);
        assert_eq!(get(&mut store, b"beta").unwrap(), Some(b"two".to_vec()));
        assert_eq!(get(&mut store, b"key-0400").unwrap(), None);
        drop(store);

        let mut reopened = ExtentFile::open(&path).unwrap();
        recover(&mut reopened, &wal).unwrap();
        assert_eq!(get(&mut reopened, b"gamma").unwrap(), Some(b"three".to_vec()));
        assert_eq!(scan_range(&mut reopened, b"alpha", b"gamma").unwrap().len(), 3);
        drop(reopened);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).ok();
    }

    #[test]
    fn replays_synced_redo_record_before_root_publication() {
        let (path, wal) = paths("redo");
        let mut store = ExtentFile::open(&path).unwrap();
        let page_id = store.allocate_extent(1).unwrap();
        let payload = encode_leaf(&[(b"durable".to_vec(), b"value".to_vec())]).unwrap();
        let frame = encode_wal(page_id, &[(page_id, payload.clone())]).unwrap();
        write_wal(&wal, &frame).unwrap();

        // Simulate a crash after the redo record is durable but before the page
        // write and root publication. Recovery must finish the commit.
        recover(&mut store, &wal).unwrap();
        assert_eq!(store.root_page_id(), Some(page_id));
        assert_eq!(get(&mut store, b"durable").unwrap(), Some(b"value".to_vec()));
        drop(store);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).ok();
    }

    #[test]
    fn validates_tree_structure_after_reopen() {
        let (path, wal) = paths("validate");
        let entries = (0..900)
            .map(|n| (format!("entry-{n:04}").into_bytes(), n.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        {
            let mut store = ExtentFile::open(&path).unwrap();
            replace_all(&mut store, &wal, &entries).unwrap();
            validate_tree(&mut store).unwrap();
        }
        let mut reopened = ExtentFile::open(&path).unwrap();
        validate_tree(&mut reopened).unwrap();
        assert_eq!(get(&mut reopened, b"entry-0899").unwrap(), Some(899u64.to_le_bytes().to_vec()));
        drop(reopened);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).ok();
    }

    #[test]
    fn bounded_range_scan_stops_at_limit_and_prunes_outside_subtrees() {
        let (path, wal) = paths("bounded-scan");
        let entries = (0..1200)
            .map(|n| (format!("key-{n:04}").into_bytes(), n.to_le_bytes().to_vec()))
            .collect::<Vec<_>>();
        let mut store = ExtentFile::open(&path).unwrap();
        replace_all(&mut store, &wal, &entries).unwrap();

        let limited = scan_range_limited(&mut store, b"key-0200", b"key-0999", Some(7)).unwrap();
        assert_eq!(limited.len(), 7);
        assert_eq!(limited.first().unwrap().0, b"key-0200");
        assert_eq!(limited.last().unwrap().0, b"key-0206");
        assert!(scan_range_limited(&mut store, b"key-0200", b"key-0999", Some(0)).unwrap().is_empty());
        assert_eq!(scan_range_limited(&mut store, b"key-0200", b"key-0202", None).unwrap().len(), 3);

        drop(store);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).ok();
    }

    #[test]
    fn rejects_unsorted_and_duplicate_keys_before_allocating() {
        let (path, wal) = paths("invalid");
        let mut store = ExtentFile::open(&path).unwrap();
        let before = store.stats().next_page_id;
        let invalid = vec![(b"b".to_vec(), vec![]), (b"a".to_vec(), vec![])];
        assert!(replace_all(&mut store, &wal, &invalid).is_err());
        assert_eq!(store.stats().next_page_id, before);
        drop(store);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).ok();
    }
}
