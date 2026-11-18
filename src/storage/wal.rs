//! Write-ahead log append and recovery.

use super::{checksum, checked_count, put_string, put_u32, storage_error, Database, DbError, Decoder};
use std::fs::{self, OpenOptions};
use std::io::Write;
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
        let bytes = fs::read(&path).map_err(storage_error)?;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let remaining = bytes.len() - offset;
            if remaining < 24 { break; } // incomplete final frame
            if &bytes[offset..offset + 4] != b"PWT2" {
                return Err(DbError::Storage(format!("invalid WAL frame header at byte {offset}")));
            }
            let sequence = u64::from_le_bytes(bytes[offset + 4..offset + 12].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[offset + 12..offset + 16].try_into().unwrap()) as usize;
            let expected = u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap());
            if len > 64 * 1024 * 1024 {
                return Err(DbError::Storage("WAL frame exceeds 64 MiB".into()));
            }
            let end = offset.checked_add(24).and_then(|n| n.checked_add(len))
                .ok_or_else(|| DbError::Storage("WAL offset overflow".into()))?;
            if end > bytes.len() { break; } // incomplete final frame
            let payload = &bytes[offset + 24..end];
            if checksum(payload) != expected {
                return Err(DbError::Storage(format!("WAL checksum mismatch at byte {offset}")));
            }
            let mut decoder = Decoder { bytes: payload, offset: 0 };
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
                for statement in statements {
                    self.execute_inner(&statement)?;
                }
                self.wal_sequence = sequence;
            }
            offset = end;
        }
        if offset < bytes.len() {
            let file = OpenOptions::new().write(true).open(&path).map_err(storage_error)?;
            file.set_len(offset as u64).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
        }
        self.revision = self.wal_sequence;
        Ok(())
    }
}

pub(super) fn wal_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".wal");
    PathBuf::from(name)
}
