//! Durable staging files shared by page-tree transaction participants.
use crate::DbError;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"PBAT";
const VERSION: u8 = 1;
const MAX_MUTATIONS: usize = 5_000;
const MAX_KEY_BYTES: usize = 1_024;
const MAX_VALUE_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedBatch {
    pub removals: Vec<Vec<u8>>,
    pub upserts: Vec<(Vec<u8>, Vec<u8>)>,
}

fn prepared_path(wal_path: &Path, transaction_id: u64) -> Result<PathBuf, DbError> {
    if transaction_id == 0 {
        return Err(DbError::Constraint("prepared transaction ID zero is reserved".into()));
    }
    let mut value = wal_path.as_os_str().to_os_string();
    value.push(format!(".prepared.{transaction_id}"));
    Ok(PathBuf::from(value))
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

pub(crate) fn write_prepared(
    wal_path: &Path,
    transaction_id: u64,
    batch: &PreparedBatch,
) -> Result<(), DbError> {
    let path = prepared_path(wal_path, transaction_id)?;
    if path.exists() {
        let existing = read_prepared(wal_path, transaction_id)?;
        if existing == *batch { return Ok(()); }
        return Err(DbError::Constraint("transaction ID already has a different prepared batch".into()));
    }
    let bytes = encode(transaction_id, batch)?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() { fs::create_dir_all(parent).map_err(storage_error)?; }
    }
    let temp = temp_path(&path);
    let result = (|| -> std::io::Result<()> {
        let mut file = OpenOptions::new().create_new(true).write(true).open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temp, &path)?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                if let Ok(dir) = File::open(parent) { dir.sync_all()?; }
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        let _ = fs::remove_file(&temp);
        return Err(storage_error(error));
    }
    Ok(())
}

pub(crate) fn read_prepared(wal_path: &Path, transaction_id: u64) -> Result<PreparedBatch, DbError> {
    let path = prepared_path(wal_path, transaction_id)?;
    let mut file = File::open(path).map_err(storage_error)?;
    let mut bytes = Vec::new();
    file.take((MAX_BATCH_BYTES + 16 * 1024 * 1024) as u64 + 1)
        .read_to_end(&mut bytes).map_err(storage_error)?;
    decode(transaction_id, &bytes)
}

pub(crate) fn remove_prepared(wal_path: &Path, transaction_id: u64) -> Result<(), DbError> {
    let path = prepared_path(wal_path, transaction_id)?;
    match fs::remove_file(&path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    if let Ok(dir) = File::open(parent) { dir.sync_all().map_err(storage_error)?; }
                }
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_error(error)),
    }
}

pub(crate) fn list_prepared(wal_path: &Path) -> Result<std::collections::BTreeSet<u64>, DbError> {
    let parent = wal_path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = wal_path.file_name().ok_or_else(|| DbError::Storage("invalid participant WAL path".into()))?;
    let prefix = format!("{}.prepared.", name.to_string_lossy());
    let mut ids = std::collections::BTreeSet::new();
    for entry in fs::read_dir(parent).map_err(storage_error)? {
        let entry = entry.map_err(storage_error)?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else { continue; };
        if let Some(suffix) = file_name.strip_prefix(&prefix) {
            let id = suffix.parse::<u64>().map_err(|_| DbError::Storage("invalid prepared transaction filename".into()))?;
            if id == 0 || !ids.insert(id) {
                return Err(DbError::Storage("duplicate or zero prepared transaction ID".into()));
            }
        }
    }
    Ok(ids)
}

fn encode(transaction_id: u64, batch: &PreparedBatch) -> Result<Vec<u8>, DbError> {
    if transaction_id == 0 || batch.removals.len() + batch.upserts.len() > MAX_MUTATIONS {
        return Err(DbError::Constraint("prepared batch exceeds the 5,000-mutation limit or has an invalid ID".into()));
    }
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&transaction_id.to_le_bytes());
    out.extend_from_slice(&(batch.removals.len() as u32).to_le_bytes());
    out.extend_from_slice(&(batch.upserts.len() as u32).to_le_bytes());
    let mut total = out.len();
    for key in &batch.removals {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(DbError::Constraint("prepared removal key must contain 1..=1024 bytes".into()));
        }
        total = total.checked_add(4 + key.len()).ok_or_else(|| DbError::Constraint("prepared batch size overflow".into()))?;
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
    }
    for (key, value) in &batch.upserts {
        if key.is_empty() || key.len() > MAX_KEY_BYTES || value.len() > MAX_VALUE_BYTES {
            return Err(DbError::Constraint("prepared upsert key or value exceeds its limit".into()));
        }
        total = total.checked_add(8 + key.len() + value.len()).ok_or_else(|| DbError::Constraint("prepared batch size overflow".into()))?;
        if total > MAX_BATCH_BYTES {
            return Err(DbError::Constraint("prepared batch exceeds 256 MiB".into()));
        }
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(value);
    }
    if out.len() > MAX_BATCH_BYTES {
        return Err(DbError::Constraint("prepared batch exceeds 256 MiB".into()));
    }
    let checksum = crc32(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

fn decode(transaction_id: u64, bytes: &[u8]) -> Result<PreparedBatch, DbError> {
    if bytes.len() < 28 || bytes.len() > MAX_BATCH_BYTES + 4
        || &bytes[..4] != MAGIC || bytes[4] != VERSION || bytes[5..8] != [0, 0, 0]
        || u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != transaction_id
        || u32::from_le_bytes(bytes[bytes.len()-4..].try_into().unwrap()) != crc32(&bytes[..bytes.len()-4]) {
        return Err(DbError::Storage("prepared participant batch is corrupt".into()));
    }
    let mut cursor = 16usize;
    let removal_count = read_u32(bytes, &mut cursor)? as usize;
    let upsert_count = read_u32(bytes, &mut cursor)? as usize;
    if removal_count + upsert_count > MAX_MUTATIONS {
        return Err(DbError::Storage("prepared participant mutation count exceeds limit".into()));
    }
    let end = bytes.len() - 4;
    let mut removals = Vec::with_capacity(removal_count);
    for _ in 0..removal_count {
        let len = read_u32(bytes, &mut cursor)? as usize;
        if len == 0 || len > MAX_KEY_BYTES || cursor.checked_add(len).is_none_or(|n| n > end) {
            return Err(DbError::Storage("invalid prepared removal key length".into()));
        }
        removals.push(bytes[cursor..cursor+len].to_vec());
        cursor += len;
    }
    let mut upserts = Vec::with_capacity(upsert_count);
    for _ in 0..upsert_count {
        let key_len = read_u32(bytes, &mut cursor)? as usize;
        let value_len = read_u32(bytes, &mut cursor)? as usize;
        if key_len == 0 || key_len > MAX_KEY_BYTES || value_len > MAX_VALUE_BYTES
            || cursor.checked_add(key_len).and_then(|n| n.checked_add(value_len)).is_none_or(|n| n > end) {
            return Err(DbError::Storage("invalid prepared upsert length".into()));
        }
        let key = bytes[cursor..cursor+key_len].to_vec();
        cursor += key_len;
        let value = bytes[cursor..cursor+value_len].to_vec();
        cursor += value_len;
        upserts.push((key, value));
    }
    if cursor != end {
        return Err(DbError::Storage("trailing bytes in prepared participant batch".into()));
    }
    Ok(PreparedBatch { removals, upserts })
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, DbError> {
    let end = cursor.checked_add(4).ok_or_else(|| DbError::Storage("prepared batch offset overflow".into()))?;
    if end > bytes.len() { return Err(DbError::Storage("truncated prepared participant batch".into())); }
    let value = u32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(value)
}
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes { crc ^= u32::from(*byte); for _ in 0..8 { let mask = 0u32.wrapping_sub(crc & 1); crc = (crc >> 1) ^ (0xedb8_8320 & mask); } }
    !crc
}
fn storage_error(error: std::io::Error) -> DbError { DbError::Storage(error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn path() -> PathBuf {
        std::env::temp_dir().join(format!("postemberdb-prepared-{}.wal",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }
    #[test]
    fn prepared_batches_round_trip_and_are_restart_discoverable() {
        let wal = path();
        let batch = PreparedBatch { removals: vec![b"old".to_vec()], upserts: vec![(b"new".to_vec(), b"value".to_vec())] };
        write_prepared(&wal, 7, &batch).unwrap();
        assert_eq!(read_prepared(&wal, 7).unwrap(), batch);
        assert_eq!(list_prepared(&wal).unwrap().into_iter().collect::<Vec<_>>(), vec![7]);
        remove_prepared(&wal, 7).unwrap();
        assert!(list_prepared(&wal).unwrap().is_empty());
        let _ = fs::remove_file(wal);
    }
}
