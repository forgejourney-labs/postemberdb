use super::{Column, DataType, Database, DbError, Row, Table, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"PDB2";
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

    /// Persist the current database state to its configured file.
    /// Writes a temporary file and renames it over the destination.
    /// Persist the current state and safely checkpoint all WAL records.
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
        let bytes = self.encode()?;
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
        if bytes.len() > 512 * 1024 * 1024 {
            return Err(DbError::Storage("database file exceeds 512 MiB safety limit".into()));
        }
        let mut input = Decoder { bytes: &bytes, offset: 0 };
        let header = input.take(4)?;
        let wal_sequence = if header == b"PDB2" {
            u64::from_le_bytes(input.take(8)?.try_into().unwrap())
        } else if header == b"PDB1" {
            0
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
            if tables.insert(name.clone(), Table { columns, rows }).is_some() {
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
