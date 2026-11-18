//! Write-ahead log append and recovery.

use super::{checksum, checked_count, put_string, put_u32, storage_error, Database, DbError, Decoder};
use std::fs::OpenOptions;
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
        // Stage the entire recovery pass once. This keeps recovery atomic while
        // avoiding a full database clone for every WAL frame.
        let mut staged = Database {
            tables: self.tables.clone(),
            storage_path: None,
            wal_path: None,
            _lock_file: None,
            wal_sequence: self.wal_sequence,
            revision: self.revision,
        };
        loop {
            let mut header = [0u8; 24];
            match file.read(&mut header[..1]).map_err(storage_error)? {
                0 => break,
                _ => {}
            }
            if let Err(error) = file.read_exact(&mut header[1..]) {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    truncated_tail = true;
                    break;
                }
                return Err(storage_error(error));
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
            if let Err(error) = file.read_exact(&mut payload) {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    truncated_tail = true;
                    break;
                }
                return Err(storage_error(error));
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
            if sequence > staged.wal_sequence {
                if sequence != staged.wal_sequence + 1 {
                    return Err(DbError::Storage(format!(
                        "WAL sequence gap: expected {}, found {sequence}", staged.wal_sequence + 1
                    )));
                }
                // Each frame is a transaction. If any statement fails, the
                // staged recovery state is discarded and self remains unchanged.
                for statement in statements {
                    staged.execute_inner(&statement)?;
                }
                staged.wal_sequence = sequence;
                staged.revision = sequence;
            }
            offset = offset.checked_add(24 + len as u64)
                .ok_or_else(|| DbError::Storage("WAL offset overflow".into()))?;
        }
        if truncated_tail {
            file.set_len(offset).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
        }
        self.tables = staged.tables;
        self.wal_sequence = staged.wal_sequence;
        self.revision = self.wal_sequence;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn recovery_rejects_corrupt_complete_frame_without_publishing() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-corrupt-{}-{nonce}.wal",
            std::process::id()
        ));
        let mut writer = Database::new();
        writer.wal_path = Some(path.clone());
        writer.append_wal_transaction(&[
            "CREATE TABLE checksum_guard (value INTEGER)".into(),
        ]).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0x01;
        fs::write(&path, bytes).unwrap();

        let mut recovered = Database::new();
        recovered.wal_path = Some(path.clone());
        let error = recovered.replay_wal().unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert!(!recovered.tables.contains_key("checksum_guard"));
        assert_eq!(recovered.wal_sequence, 0);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn recovery_rejects_wal_sequence_gap() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-gap-{}-{nonce}.wal",
            std::process::id()
        ));
        let mut writer = Database::new();
        writer.wal_path = Some(path.clone());
        writer.wal_sequence = 1;
        writer.append_wal_transaction(&[
            "CREATE TABLE gap_guard (value INTEGER)".into(),
        ]).unwrap();

        let mut recovered = Database::new();
        recovered.wal_path = Some(path.clone());
        let error = recovered.replay_wal().unwrap_err();
        assert!(error.to_string().contains("sequence gap"));
        assert!(!recovered.tables.contains_key("gap_guard"));
        assert_eq!(recovered.wal_sequence, 0);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_recovery_does_not_publish_earlier_frames() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-pass-atomic-{}-{nonce}.wal",
            std::process::id()
        ));

        let mut writer = Database::new();
        writer.wal_path = Some(path.clone());
        writer.append_wal_transaction(&[
            "CREATE TABLE first_frame (value INTEGER)".into(),
        ]).unwrap();
        writer.wal_sequence = 1;
        writer.append_wal_transaction(&[
            "INSERT INTO missing_table VALUES (1)".into(),
        ]).unwrap();

        let mut recovered = Database::new();
        recovered.wal_path = Some(path.clone());
        assert!(recovered.replay_wal().is_err());
        assert!(!recovered.tables.contains_key("first_frame"));
        assert_eq!(recovered.wal_sequence, 0);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn replay_truncates_an_incomplete_payload_without_applying_it() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-torn-payload-{}-{nonce}.wal",
            std::process::id()
        ));

        let mut writer = Database::new();
        writer.wal_path = Some(path.clone());
        writer.append_wal_transaction(&[
            "CREATE TABLE incomplete (value INTEGER)".into(),
        ]).unwrap();
        let length = fs::metadata(&path).unwrap().len();
        OpenOptions::new().write(true).open(&path).unwrap()
            .set_len(length - 1).unwrap();

        let mut recovered = Database::new();
        recovered.wal_path = Some(path.clone());
        recovered.replay_wal().unwrap();
        assert!(!recovered.tables.contains_key("incomplete"));
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);

        fs::remove_file(path).unwrap();
    }

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
