use super::{Column, DataType, Database, DbError, Row, Table, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"PDB3";
const PAGE_MAGIC: &[u8; 4] = b"PPG1";
const PAGE_SIZE: usize = 4096;
const PAGE_META_SIZE: usize = 16;
const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_META_SIZE;
const MAX_SNAPSHOT_SIZE: usize = 512 * 1024 * 1024;
const MAX_ITEMS: usize = 1_000_000;
const MAX_STRING: usize = 64 * 1024 * 1024;

impl Database {
    /// Open a persistent database. A missing file creates a new empty database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let mut db = if path.exists() {
            Self::decode_file(&path)?
        } else {
            Self::new()
        };
        db.storage_path = Some(path.clone());
        db.wal_path = Some(wal_file_path(&path));
        if !path.exists() {
            db.write_snapshot()?;
        }
        db.replay_wal()?;
        Ok(db)
    }

    /// Persist a snapshot and safely checkpoint all WAL records.
    /// The snapshot records the last applied WAL sequence, so a crash between
    /// the snapshot rename and WAL truncation cannot replay committed writes twice.
    pub fn save(&self) -> Result<(), DbError> {
        self.write_snapshot()?;
        if let Some(path) = &self.wal_path {
            let file = OpenOptions::new().create(true).write(true).truncate(true).open(path).map_err(storage_error)?;
            file.sync_all().map_err(storage_error)?;
        }
        Ok(())
    }

    fn write_snapshot(&self) -> Result<(), DbError> {
        let path = self.storage_path.as_ref()
            .ok_or_else(|| DbError::Storage("database has no storage path; open it with Database::open".into()))?;
        let payload = self.encode()?;
        let bytes = encode_pages(&payload)?;
        let temp = temporary_path(path);
        let result = (|| -> io::Result<()> {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() { fs::create_dir_all(parent)?; }
            }
            let mut file = File::create(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp, path)?;
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    if let Ok(dir) = File::open(parent) { let _ = dir.sync_all(); }
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = fs::remove_file(&temp);
            return Err(DbError::Storage(error.to_string()));
        }
        Ok(())
    }

    pub(super) fn append_wal_transaction(&self, statements: &[String]) -> Result<u64, DbError> {
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

    fn replay_wal(&mut self) -> Result<(), DbError> {
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
        Ok(())
    }

    /// Flush the current state to a snapshot and truncate the WAL safely.
    pub fn checkpoint(&self) -> Result<(), DbError> {
        self.save()
    }

    fn encode(&self) -> Result<Vec<u8>, DbError> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.wal_sequence.to_le_bytes());
        put_u32(&mut out, checked_count(self.tables.len())?);
        for (name, table) in &self.tables {
            put_string(&mut out, name)?;
            put_u32(&mut out, checked_count(table.columns.len())?);
            for column in &table.columns {
                put_string(&mut out, &column.name)?;
                out.push(match column.data_type { DataType::Integer => 1, DataType::Text => 2, DataType::Boolean => 3 });
                out.push(u8::from(column.nullable));
            }
            put_u32(&mut out, checked_count(table.indexes.len())?);
            for (index_name, index) in &table.indexes {
                put_string(&mut out, index_name)?;
                let column = table.columns.get(index.column_index)
                    .ok_or_else(|| DbError::Storage("index column is outside table schema".into()))?;
                put_string(&mut out, &column.name)?;
            }
            put_u32(&mut out, checked_count(table.rows.len())?);
            for row in &table.rows {
                if row.values.len() != table.columns.len() {
                    return Err(DbError::Storage(format!("row width mismatch in table {name}")));
                }
                for value in &row.values {
                    match value {
                        Value::Null => out.push(0),
                        Value::Integer(v) => { out.push(1); out.extend_from_slice(&v.to_le_bytes()); }
                        Value::Text(v) => { out.push(2); put_string(&mut out, v)?; }
                        Value::Boolean(v) => { out.push(3); out.push(u8::from(*v)); }
                    }
                }
            }
        }
        Ok(out)
    }

    fn decode_file(path: &Path) -> Result<Self, DbError> {
        let file = File::open(path).map_err(storage_error)?;
        let mut bytes = Vec::new();
        let mut limited = file.take((512 * 1024 * 1024) as u64 + 1);
        limited.read_to_end(&mut bytes).map_err(storage_error)?;
        if bytes.len() > MAX_SNAPSHOT_SIZE + 64 * 1024 * 1024 {
            return Err(DbError::Storage("database file exceeds snapshot safety limit".into()));
        }
        let payload = if bytes.starts_with(PAGE_MAGIC) {
            decode_pages(&bytes)?
        } else {
            // Backward compatibility with pre-page PDB1/PDB2/PDB3 snapshots.
            bytes
        };
        let bytes = payload;
        let mut input = Decoder { bytes: &bytes, offset: 0 };
        let header = input.take(4)?;
        let (format_version, wal_sequence) = if header == b"PDB3" {
            (3, u64::from_le_bytes(input.take(8)?.try_into().unwrap()))
        } else if header == b"PDB2" {
            (2, u64::from_le_bytes(input.take(8)?.try_into().unwrap()))
        } else if header == b"PDB1" {
            (1, 0)
        } else {
            return Err(DbError::Storage("invalid database file header".into()));
        };
        let table_count = input.count()?;
        let mut tables = BTreeMap::new();
        for _ in 0..table_count {
            let name = input.string()?;
            let column_count = input.count()?;
            if column_count == 0 { return Err(DbError::Storage("table has no columns".into())); }
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                let column_name = input.string()?;
                let data_type = match input.byte()? {
                    1 => DataType::Integer,
                    2 => DataType::Text,
                    3 => DataType::Boolean,
                    _ => return Err(DbError::Storage("unknown column type in database file".into())),
                };
                let nullable = match input.byte()? {
                    0 => false, 1 => true,
                    _ => return Err(DbError::Storage("invalid nullable flag in database file".into())),
                };
                columns.push(Column { name: column_name, data_type, nullable });
            }
            let mut index_definitions = Vec::new();
            if format_version >= 3 {
                let index_count = input.count()?;
                for _ in 0..index_count {
                    index_definitions.push((input.string()?, input.string()?));
                }
            }
            let row_count = input.count()?;
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let mut values = Vec::with_capacity(column_count);
                for column in &columns {
                    let value = match input.byte()? {
                        0 => Value::Null,
                        1 => Value::Integer(i64::from_le_bytes(input.take(8)?.try_into().unwrap())),
                        2 => Value::Text(input.string()?),
                        3 => match input.byte()? {
                            0 => Value::Boolean(false), 1 => Value::Boolean(true),
                            _ => return Err(DbError::Storage("invalid boolean value".into())),
                        },
                        _ => return Err(DbError::Storage("unknown value tag in database file".into())),
                    };
                    let valid = matches!((&column.data_type, &value),
                        (_, Value::Null) if column.nullable)
                        || matches!((&column.data_type, &value),
                            (DataType::Integer, Value::Integer(_)) |
                            (DataType::Text, Value::Text(_)) |
                            (DataType::Boolean, Value::Boolean(_)));
                    if !valid { return Err(DbError::Storage(format!("invalid value for column {}", column.name))); }
                    values.push(value);
                }
                rows.push(Row { values });
            }
            let mut table = Table { columns, rows, indexes: BTreeMap::new() };
            for (index_name, column_name) in index_definitions {
                table.add_index(index_name, &column_name)?;
            }
            if tables.insert(name.clone(), table).is_some() {
                return Err(DbError::Storage(format!("duplicate table {name} in database file")));
            }
        }
        if input.offset != bytes.len() {
            return Err(DbError::Storage("trailing bytes in database file".into()));
        }
        Ok(Database { tables, storage_path: None, wal_path: None, wal_sequence })
    }
}

fn storage_error(error: io::Error) -> DbError { DbError::Storage(error.to_string()) }

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

fn checked_count(count: usize) -> Result<u32, DbError> {
    if count > MAX_ITEMS { return Err(DbError::Storage("too many items to store".into())); }
    Ok(count as u32)
}

fn put_u32(out: &mut Vec<u8>, value: u32) { out.extend_from_slice(&value.to_le_bytes()); }

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), DbError> {
    if value.len() > MAX_STRING { return Err(DbError::Storage("string exceeds storage limit".into())); }
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Decoder<'a> { bytes: &'a [u8], offset: usize }

impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], DbError> {
        let end = self.offset.checked_add(count)
            .ok_or_else(|| DbError::Storage("database file offset overflow".into()))?;
        if end > self.bytes.len() { return Err(DbError::Storage("truncated database file".into())); }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8, DbError> { Ok(self.take(1)?[0]) }
    fn count(&mut self) -> Result<usize, DbError> {
        let value = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        if value > MAX_ITEMS { return Err(DbError::Storage("database item count exceeds safety limit".into())); }
        Ok(value)
    }
    fn string(&mut self) -> Result<String, DbError> {
        let len = self.count_string_len()?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| DbError::Storage("database contains invalid UTF-8".into()))
    }
    fn count_string_len(&mut self) -> Result<usize, DbError> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        if len > MAX_STRING { return Err(DbError::Storage("stored string exceeds safety limit".into())); }
        Ok(len)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("postemberdb-{label}-{}-{nonce}.pdb", std::process::id()))
    }

    #[test]
    fn database_reopens_with_persisted_rows() {
        let path = test_path("roundtrip");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT, active BOOLEAN)").unwrap();
            db.execute("INSERT INTO users VALUES (7, 'Ada', TRUE)").unwrap();
            db.execute("INSERT INTO users VALUES (8, 'Lin', FALSE)").unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values, vec![
            Value::Integer(7), Value::Text("Ada".into()), Value::Boolean(true)
        ]);
        assert_eq!(result.rows[1].values, vec![
            Value::Integer(8), Value::Text("Lin".into()), Value::Boolean(false)
        ]);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_invalid_database_header() {
        let path = test_path("invalid");
        fs::write(&path, b"NOPE").unwrap();
        let error = Database::open(&path).unwrap_err();
        assert!(error.to_string().contains("invalid database file header"));
        fs::remove_file(path).unwrap();
    }


    #[test]
    fn checkpoint_does_not_replay_committed_wal_twice() {
        let path = test_path("checkpoint");
        let wal = wal_file_path(&path);
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE checkpointed (value INTEGER)").unwrap();
            db.execute("INSERT INTO checkpointed VALUES (42)").unwrap();
            db.checkpoint().unwrap();
            assert_eq!(fs::metadata(&wal).unwrap().len(), 0);
        }
        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM checkpointed").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Integer(42));
        drop(reopened);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).unwrap();
    }

    #[test]
    fn recovery_discards_an_incomplete_final_wal_frame() {
        let path = test_path("torn-wal");
        let wal = wal_file_path(&path);
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE torn (value INTEGER)").unwrap();
            db.execute("INSERT INTO torn VALUES (9)").unwrap();
        }
        let valid_len = fs::metadata(&wal).unwrap().len();
        OpenOptions::new().append(true).open(&wal).unwrap().write_all(b"PWT2partial").unwrap();
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.execute("SELECT * FROM torn").unwrap().rows.len(), 1);
        assert_eq!(fs::metadata(&wal).unwrap().len(), valid_len);
        drop(reopened);
        fs::remove_file(path).unwrap();
        fs::remove_file(wal).unwrap();
    }


    #[test]
    fn page_snapshot_round_trips_across_multiple_pages() {
        let path = test_path("pages");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE page_data (value TEXT)").unwrap();
            let rows = (0..300).map(|n| vec![Value::Text(format!("row-{n}-{}", "x".repeat(40)))]).collect::<Vec<_>>();
            db.insert_many("page_data", rows).unwrap();
            db.checkpoint().unwrap();
        }
        let file = fs::read(&path).unwrap();
        assert_eq!(&file[..4], PAGE_MAGIC);
        assert!(u32::from_le_bytes(file[12..16].try_into().unwrap()) > 1);
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.execute("SELECT * FROM page_data").unwrap().rows.len(), 300);
        drop(reopened);
        fs::remove_file(path).unwrap();
        fs::remove_file(test_path("unused")).ok();
    }

    #[test]
    fn page_snapshot_detects_corrupted_page_payload() {
        let path = test_path("page-corrupt");
        {
            let db = Database::open(&path).unwrap();
            db.save().unwrap();
        }
        let mut bytes = fs::read(&path).unwrap();
        bytes[32 + PAGE_META_SIZE] ^= 0x40;
        fs::write(&path, bytes).unwrap();
        let error = Database::open(&path).unwrap_err();
        assert!(error.to_string().contains("checksum"));
        fs::remove_file(path).unwrap();
        fs::remove_file(wal_file_path(&test_path("unused"))).ok();
    }

    #[test]
    fn failed_statement_does_not_replace_persisted_state() {
        let path = test_path("failed-write");
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE counters (value INTEGER)").unwrap();
            assert!(db.execute("INSERT INTO counters VALUES ('not-an-integer')").is_err());
        }
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.execute("SELECT * FROM counters").unwrap().rows.len(), 0);
        drop(reopened);
        fs::remove_file(path).unwrap();
    }
}



fn encode_pages(payload: &[u8]) -> Result<Vec<u8>, DbError> {
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

fn decode_pages(file: &[u8]) -> Result<Vec<u8>, DbError> {
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

fn wal_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".wal");
    PathBuf::from(name)
}

fn checksum(bytes: &[u8]) -> u64 {
    // FNV-1a detects accidental corruption; it is not a cryptographic MAC.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}


fn wal_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".wal");
    PathBuf::from(name)
}

fn checksum(bytes: &[u8]) -> u64 {
    // FNV-1a detects accidental corruption; it is not a cryptographic MAC.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
