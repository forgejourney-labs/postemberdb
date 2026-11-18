//! Fixed-size page-file encoding and validation.

use super::{checksum, DbError, MAX_SNAPSHOT_SIZE, PAGE_MAGIC, PAGE_META_SIZE, PAGE_PAYLOAD_SIZE, PAGE_SIZE};

pub(super) fn encode_pages(payload: &[u8]) -> Result<Vec<u8>, DbError> {
    if payload.len() > MAX_SNAPSHOT_SIZE {
        return Err(DbError::Storage("snapshot exceeds 512 MiB safety limit".into()));
    }
    let page_count = payload.len().div_ceil(PAGE_PAYLOAD_SIZE);
    let page_count_u32 = u32::try_from(page_count)
        .map_err(|_| DbError::Storage("snapshot requires too many pages".into()))?;
    let mut out = Vec::with_capacity(32 + page_count * PAGE_SIZE);
    out.extend_from_slice(PAGE_MAGIC);
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    out.extend_from_slice(&page_count_u32.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&checksum(payload).to_le_bytes());

    for (page_number, chunk) in payload.chunks(PAGE_PAYLOAD_SIZE).enumerate() {
        out.extend_from_slice(&(page_number as u32).to_le_bytes());
        out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        out.extend_from_slice(&checksum(chunk).to_le_bytes());
        out.extend_from_slice(chunk);
        out.resize(out.len() + PAGE_PAYLOAD_SIZE - chunk.len(), 0);
    }
    Ok(out)
}

pub(super) fn decode_pages(file: &[u8]) -> Result<Vec<u8>, DbError> {
    if file.len() < 32 || &file[..4] != PAGE_MAGIC {
        return Err(DbError::Storage("invalid page-file header".into()));
    }
    let version = u32::from_le_bytes(file[4..8].try_into().unwrap());
    let page_size = u32::from_le_bytes(file[8..12].try_into().unwrap()) as usize;
    let page_count = u32::from_le_bytes(file[12..16].try_into().unwrap()) as usize;
    let payload_len = u64::from_le_bytes(file[16..24].try_into().unwrap()) as usize;
    let expected_checksum = u64::from_le_bytes(file[24..32].try_into().unwrap());

    if version != 1 || page_size != PAGE_SIZE {
        return Err(DbError::Storage("unsupported page-file version or page size".into()));
    }
    if payload_len > MAX_SNAPSHOT_SIZE {
        return Err(DbError::Storage("page-file payload exceeds 512 MiB safety limit".into()));
    }
    let expected_pages = payload_len.div_ceil(PAGE_PAYLOAD_SIZE);
    if page_count != expected_pages {
        return Err(DbError::Storage("page count does not match payload length".into()));
    }
    let expected_file_len = 32usize.checked_add(page_count.checked_mul(PAGE_SIZE)
        .ok_or_else(|| DbError::Storage("page-file size overflow".into()))?)
        .ok_or_else(|| DbError::Storage("page-file size overflow".into()))?;
    if file.len() != expected_file_len {
        return Err(DbError::Storage("page-file length mismatch".into()));
    }

    let mut payload = Vec::with_capacity(payload_len);
    for page_number in 0..page_count {
        let start = 32 + page_number * PAGE_SIZE;
        let page_id = u32::from_le_bytes(file[start..start + 4].try_into().unwrap()) as usize;
        let used = u32::from_le_bytes(file[start + 4..start + 8].try_into().unwrap()) as usize;
        let page_checksum = u64::from_le_bytes(file[start + 8..start + 16].try_into().unwrap());
        if page_id != page_number || used > PAGE_PAYLOAD_SIZE {
            return Err(DbError::Storage(format!("invalid page metadata for page {page_number}")));
        }
        let data = &file[start + PAGE_META_SIZE..start + PAGE_META_SIZE + used];
        if checksum(data) != page_checksum {
            return Err(DbError::Storage(format!("page checksum mismatch for page {page_number}")));
        }
        payload.extend_from_slice(data);
    }
    if payload.len() != payload_len || checksum(&payload) != expected_checksum {
        return Err(DbError::Storage("page-file payload checksum mismatch".into()));
    }
    Ok(payload)
}
