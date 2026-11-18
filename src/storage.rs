use super::{Column, DataType, Database, DbError, Row, Table, Value};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use fs2::FileExt;

mod allocator;
pub(crate) mod btree;
pub(crate) mod extent;
pub(crate) mod index_codec;
pub(crate) mod page_tree;
mod pages;
mod wal;

use pages::{decode_pages, encode_pages};
use wal::wal_file_path;

const MAGIC: &[u8; 4] = b"PDB4";
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
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(storage_error)?;
            }
        }
        let lock_path = lock_file_path(&path);
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(storage_error)?;
        FileExt::try_lock_exclusive(&lock_file)
            .map_err(|error| DbError::Storage(format!("database is already open by another process or handle: {error}")))?;
        let mut db = if path.exists() {
            Self::decode_file(&path)?
        } else {
            Self::new()
        };
        db.storage_path = Some(path.clone());
        db.wal_path = Some(wal_file_path(&path));
        db._lock_file = Some(std::sync::Arc::new(lock_file));
        if !path.exists() {
            db.write_snapshot()?;
        }
        db.replay_wal()?;
        recover_page_tree(&path)?;
        db.sync_ember_indexes()?;
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
                out.push(u8::from(index.unique));
                put_u32(&mut out, checked_count(index.column_indices.len())?);
                for column_index in &index.column_indices {
                    let column = table.columns.get(*column_index)
                        .ok_or_else(|| DbError::Storage("index column is outside table schema".into()))?;
                    put_string(&mut out, &column.name)?;
                }
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
        let mut limited = file.take((MAX_SNAPSHOT_SIZE + 16 * 1024 * 1024) as u64 + 1);
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
        let (format_version, wal_sequence) = if header == b"PDB4" {
            (4, u64::from_le_bytes(input.take(8)?.try_into().unwrap()))
        } else if header == b"PDB3" {
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
            if format_version >= 4 {
                let index_count = input.count()?;
                for _ in 0..index_count {
                    let index_name = input.string()?;
                    let unique = match input.byte()? {
                        0 => false, 1 => true,
                        _ => return Err(DbError::Storage("invalid unique-index flag".into())),
                    };
                    let index_columns = input.count()?;
                    if index_columns == 0 { return Err(DbError::Storage("index has no columns".into())); }
                    let mut column_names = Vec::with_capacity(index_columns);
                    for _ in 0..index_columns { column_names.push(input.string()?); }
                    index_definitions.push((index_name, column_names, unique));
                }
            } else if format_version == 3 {
                let index_count = input.count()?;
                for _ in 0..index_count {
                    index_definitions.push((input.string()?, vec![input.string()?], false));
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
            for (index_name, column_names, unique) in index_definitions {
                table.add_index_columns(index_name, &column_names, unique)?;
            }
            if tables.insert(name.clone(), table).is_some() {
                return Err(DbError::Storage(format!("duplicate table {name} in database file")));
            }
        }
        if input.offset != bytes.len() {
            return Err(DbError::Storage("trailing bytes in database file".into()));
        }
        Ok(Database { tables, storage_path: None, wal_path: None, _lock_file: None, wal_sequence, revision: wal_sequence })
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
        std::env::temp_dir().join(format!("postemberdb-{label}-{}-{nonce}.pbd", std::process::id()))
    }

    #[test]
    fn committed_sql_indexes_are_mirrored_into_persistent_ember_tree() {
        let path = test_path("ember-index-mirror");
        let page_path = page_tree_file_path(&path);
        let tree_wal_path = page_tree_wal_file_path(&path);
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE players (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("CREATE INDEX idx_players_uuid ON players (uuid)").unwrap();
            db.execute("INSERT INTO players VALUES ('player-a', 10)").unwrap();
            db.execute("INSERT INTO players VALUES ('player-a', 20)").unwrap();
        }

        let mut store = extent::ExtentFile::open(&page_path).unwrap();
        let mut disk_key = Vec::new();
        put_namespace_part(&mut disk_key, b"players").unwrap();
        put_namespace_part(&mut disk_key, b"idx_players_uuid").unwrap();
        disk_key.extend_from_slice(&index_codec::encode_index_key(
            &super::super::IndexKey::from(&Value::Text("player-a".into()))
        ).unwrap());
        let mut lower = disk_key.clone();
        lower.push(0);
        lower.extend_from_slice(&0u64.to_be_bytes());
        let mut upper = disk_key.clone();
        upper.push(0);
        upper.extend_from_slice(&u64::MAX.to_be_bytes());
        let refs = page_tree::scan_range(&mut store, &lower, &upper).unwrap()
            .into_iter()
            .map(|(key, _)| u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        assert_eq!(refs, vec![0, 1]);
        drop(store);

        for candidate in [&path, &page_path, &tree_wal_path, &wal_file_path(&path), &lock_file_path(&path)] {
            let _ = fs::remove_file(candidate);
        }
    }

    #[test]
    fn sql_wal_rebuilds_missing_ember_index_sidecar_on_open() {
        let path = test_path("ember-index-rebuild");
        let page_path = page_tree_file_path(&path);
        let tree_wal_path = page_tree_wal_file_path(&path);
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE rebuild_rows (id INTEGER, label TEXT)").unwrap();
            db.execute("CREATE INDEX idx_rebuild_label ON rebuild_rows (label)").unwrap();
            db.execute("INSERT INTO rebuild_rows VALUES (7, 'durable')").unwrap();
        }
        fs::remove_file(&page_path).unwrap();

        let reopened = Database::open(&path).unwrap();
        let mut store = extent::ExtentFile::open(&page_path).unwrap();
        let mut disk_key = Vec::new();
        put_namespace_part(&mut disk_key, b"rebuild_rows").unwrap();
        put_namespace_part(&mut disk_key, b"idx_rebuild_label").unwrap();
        disk_key.extend_from_slice(&index_codec::encode_index_key(
            &super::super::IndexKey::from(&Value::Text("durable".into()))
        ).unwrap());
        let mut lower = disk_key.clone();
        lower.push(0);
        lower.extend_from_slice(&0u64.to_be_bytes());
        let mut upper = disk_key.clone();
        upper.push(0);
        upper.extend_from_slice(&u64::MAX.to_be_bytes());
        let refs = page_tree::scan_range(&mut store, &lower, &upper).unwrap()
            .into_iter()
            .map(|(key, _)| u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()) as usize)
            .collect::<Vec<_>>();
        assert_eq!(refs, vec![0]);
        assert_eq!(reopened.tables["rebuild_rows"].rows.len(), 1);
        drop(store);
        drop(reopened);

        for candidate in [&path, &page_path, &tree_wal_path, &wal_file_path(&path), &lock_file_path(&path)] {
            let _ = fs::remove_file(candidate);
        }
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
    fn legacy_db_extension_reads_old_snapshot_and_checkpoints_to_current_format() {
        let path = test_path("legacy-db").with_extension("db");
        let mut legacy = b"PDB1".to_vec();
        legacy.extend_from_slice(&0u32.to_le_bytes());
        fs::write(&path, legacy).unwrap();

        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE legacy_rows (id INTEGER, label TEXT)").unwrap();
            db.execute("INSERT INTO legacy_rows VALUES (1, 'kept')").unwrap();
            db.checkpoint().unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM legacy_rows").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], Value::Text("kept".into()));
        drop(reopened);

        assert!(fs::read(&path).unwrap().starts_with(PAGE_MAGIC));
        fs::remove_file(&path).unwrap();
        fs::remove_file(wal_file_path(&path)).ok();
        fs::remove_file(lock_file_path(&path)).ok();
    }

    #[test]
    fn database_open_recovers_an_existing_ember_tree_sidecar() {
        let path = test_path("embertree-recovery");
        let pages = page_tree_file_path(&path);
        let tree_wal = page_tree_wal_file_path(&path);
        {
            let _store = extent::ExtentFile::open(&pages).unwrap();
            fs::write(&tree_wal, b"PBTWpartial").unwrap();
        }

        let db = Database::open(&path).unwrap();
        drop(db);
        assert_eq!(fs::metadata(&tree_wal).unwrap().len(), 0);

        fs::remove_file(&path).ok();
        fs::remove_file(wal_file_path(&path)).ok();
        fs::remove_file(lock_file_path(&path)).ok();
        fs::remove_file(pages).ok();
        fs::remove_file(tree_wal).ok();
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
        fs::remove_file(&path).unwrap();
        fs::remove_file(wal_file_path(&path)).ok();
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
        fs::remove_file(&path).unwrap();
        fs::remove_file(wal_file_path(&path)).ok();
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



fn checksum(bytes: &[u8]) -> u64 {
    // FNV-1a detects accidental corruption; it is not a cryptographic MAC.
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}


fn page_tree_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".pages");
    PathBuf::from(name)
}

fn page_tree_wal_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".pages.btree.wal");
    PathBuf::from(name)
}

/// Rebuild the durable EmberTree index mirror from the SQL-WAL-recovered state.
/// SQL snapshots/WAL remain authoritative; this operation is idempotent and repairable.
impl Database {
    pub(crate) fn sync_ember_indexes(&self) -> Result<(), DbError> {
        let Some(database_path) = self.storage_path.as_deref() else { return Ok(()); };
        let page_path = page_tree_file_path(database_path);
        let tree_wal_path = page_tree_wal_file_path(database_path);
        let mut entries = BTreeMap::<Vec<u8>, Vec<usize>>::new();

        for (table_name, table) in &self.tables {
            for (index_name, index) in &table.indexes {
                let upper = super::IndexKey::Composite(vec![super::IndexAtom::Boolean(true); 256]);
                for (key, rows) in index.entries.scan_range(&super::IndexKey::Null, &upper) {
                    let encoded = index_codec::encode_index_key(&key)?;
                    let mut disk_key = Vec::with_capacity(4 + table_name.len() + index_name.len() + encoded.len());
                    put_namespace_part(&mut disk_key, table_name.as_bytes())?;
                    put_namespace_part(&mut disk_key, index_name.as_bytes())?;
                    disk_key.extend_from_slice(&encoded);
                    if disk_key.len() > 1015 {
                        return Err(DbError::Storage("namespaced SQL index key leaves no room for its row-reference suffix".into()));
                    }
                    for row in rows {
                        let row = u64::try_from(row)
                            .map_err(|_| DbError::Storage("SQL row reference does not fit in u64".into()))?;
                        let mut entry_key = disk_key.clone();
                        entry_key.push(0);
                        entry_key.extend_from_slice(&row.to_be_bytes());
                        entries.insert(entry_key, Vec::new());
                    }
                }
            }
        }

        if entries.is_empty() && !page_path.exists() && !tree_wal_path.exists() {
            return Ok(());
        }
        let mut store = extent::ExtentFile::open(&page_path)?;
        if entries.is_empty() {
            store.publish_root(None)?;
            return Ok(());
        }
        let encoded = entries.into_iter().collect::<Vec<_>>();
        page_tree::replace_all(&mut store, &tree_wal_path, &encoded)
    }
}

fn put_namespace_part(out: &mut Vec<u8>, value: &[u8]) -> Result<(), DbError> {
    let length = u16::try_from(value.len())
        .map_err(|_| DbError::Storage("SQL table or index name exceeds 65535 bytes".into()))?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

/// Recover the experimental EmberTree sidecar before exposing the database.
/// The main PPG1 snapshot and SQL WAL remain untouched and authoritative.
fn recover_page_tree(path: &Path) -> Result<(), DbError> {
    let page_path = page_tree_file_path(path);
    let wal_path = page_tree_wal_file_path(path);
    if !page_path.exists() && !wal_path.exists() {
        return Ok(());
    }
    let mut store = extent::ExtentFile::open(&page_path)?;
    page_tree::recover(&mut store, &wal_path)
}

fn lock_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}
