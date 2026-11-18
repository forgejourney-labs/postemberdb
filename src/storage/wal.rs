//! Write-ahead log append and recovery.

use super::{checksum, checked_count, put_string, put_u32, storage_error, Database, DbError, Decoder};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

impl Database {
    pub(crate) fn append_wal_transaction(&self, statements: &[String]) -> Result<u64, DbError> {
        let path = self.wal_path.as_ref()
            .ok_or_else(|| DbError::Storage("database has no WAL path".into()))?;
        let mut payload = Vec::new();
        put_u32(&mut payload, checked_count(statements.len())?);
        for statement in statements {
            put_string(&mut payload, statement)?;
        }
        if payload.len() > 64 * 1024 * 1024 {
            return Err(DbError::Storage("WAL transaction exceeds 64 MiB".into()));
        }
        let sequence = self.wal_sequence.checked_add(1)
            .ok_or_else(|| DbError::Storage("WAL sequence exhausted".into()))?;
        let mut frame = Vec::with_capacity(24 + payload.len());
        frame.extend_from_slice(b"PWT2");
        frame.extend_from_slice(&sequence.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);

        let mut file = OpenOptions::new().create(true).append(true).open(path).map_err(storage_error)?;
        file.write_all(&frame).map_err(storage_error)?;
        file.sync_all().map_err(storage_error)?;
        Ok(sequence)
    }

    pub(super) fn replay_wal(&mut self) -> Result<(), DbError> {
        let Some(path) = self.wal_path.clone() else { return Ok(()); };
        if !path.exists() { return Ok(()); }
        // Stream frames instead of loading the complete WAL into memory.
        // Only one bounded (maximum 64 MiB) transaction payload is resident.
        let mut file = OpenOptions::new().read(true).write(true).open(&path).map_err(storage_error)?;
        let mut offset = 0u64;
        let mut truncated_tail = false;
        loop {
            let mut header = [0u8; 24];
            match file.read(&mut header[..1]).map_err(storage_error)? {
                0 => break,
                _ => {}
            }
            if file.read_exact(&mut header[1..]).is_err() {
                truncated_tail = true;
                break;
            }
            if &header[..4] != b"PWT2" {
                return Err(DbError::Storage(format!("invalid WAL frame header at byte {offset}")));
            }
            let sequence = u64::from_le_bytes(header[4..12].try_into().unwrap());
            let len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
            let expected = u64::from_le_bytes(header[16..24].try_into().unwrap());
            if len > 64 * 1024 * 1024 {
                return Err(DbError::Storage("WAL frame exceeds 64 MiB".into()));
            }
            let mut payload = vec![0u8; len];
            if file.read_exact(&mut payload).is_err() {
                truncated_tail = true;
                break;
            }
            if checksum(&payload) != expected {
                return Err(DbError::Storage(format!("WAL checksum mismatch at byte {offset}")));
            }
            let mut decoder = Decoder { bytes: &payload, offset: 0 };
            let count = decoder.count()?;
            let mut statements = Vec::with_capacity(count);
            for _ in 0..count { statements.push(decoder.string()?); }
            if decoder.offset != payload.len() {
                return Err(DbError::Storage("trailing bytes in WAL transaction".into()));
            }
            if sequence > self.wal_sequence {
                if sequence != self.wal_sequence + 1 {
                    return Err(DbError::Storage(format!(
                        "WAL sequence gap: expected {}, found {sequence}", self.wal_sequence + 1
                    )));
                }
                // A WAL frame is one committed transaction. Replay it against a
                // private copy so a malformed later statement cannot publish a
                // partially-applied transaction during recovery.
                let mut staged = Database {
                    tables: self.tables.clone(),
                    storage_path: None,
                    wal_path: None,
                    _lock_file: None,
                    wal_sequence: self.wal_sequence,
                    revision: self.revision,
                };
                for statement in statements {
                    staged.execute_inner(&statement)?;
                }
                self.tables = staged.tables;
                self.wal_sequence = sequence;
                self.revision = sequence;
            }
            offset = offset.checked_add(24 + len as u64)
                .ok_or_else(|| DbError::Storage("WAL offset overflow".into()))?;
        }
        if truncated_tail {
            file.set_len(offset).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
        }
        self.revision = self.wal_sequence;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn replay_is_atomic_per_wal_transaction() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-atomic-{}-{nonce}.wal",
            std::process::id()
        ));

        let mut writer = Database::new();
        writer.wal_path = Some(path.clone());
        writer.append_wal_transaction(&[
            "CREATE TABLE should_not_exist (value INTEGER)".into(),
            "INSERT INTO missing_table VALUES (1)".into(),
        ]).unwrap();

        let mut recovered = Database::new();
        recovered.wal_path = Some(path.clone());
        assert!(recovered.replay_wal().is_err());
        assert!(!recovered.tables.contains_key("should_not_exist"));
        assert_eq!(recovered.wal_sequence, 0);

        fs::remove_file(path).unwrap();
    }
}

pub(super) fn wal_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".wal");
    PathBuf::from(name)
}
