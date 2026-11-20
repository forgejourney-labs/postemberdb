use super::{validate_value, Column, DataType, Database, DbError, IndexKey, Row, Table, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use fs2::FileExt;

mod allocator;
pub(crate) mod cache;
pub(crate) mod btree;
pub(crate) mod extent;
pub(crate) mod index_codec;
pub(crate) mod page_tree;
pub mod record_store;
pub mod sql_rows;
mod pages;
mod pdb6;
pub(crate) mod pdb6_store;
pub mod emberforge;
pub mod emberforge7;
mod wal;
pub mod transaction_coordinator;
pub(crate) mod participant;

use pages::{decode_pages, encode_pages};
use wal::wal_file_path;
use pdb6::{CatalogColumn, CatalogIndex, CatalogTable, DatabaseManifest};
use pdb6_store::Pdb6Store;

const MAGIC: &[u8; 4] = b"PDB5";
const PAGE_MAGIC: &[u8; 4] = b"PPG1";
const PAGE_SIZE: usize = 4096;
const PAGE_META_SIZE: usize = 16;
const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_META_SIZE;
const MAX_SNAPSHOT_SIZE: usize = 512 * 1024 * 1024;
const MAX_ITEMS: usize = 1_000_000;
const MAX_STRING: usize = 64 * 1024 * 1024;

impl Database {
    /// Creates a new empty PDB6 database and makes its page store the SQL authority.
    ///
    /// Existing database files and PDB6 sidecars are never overwritten. Use
    /// migrate_to_pdb6 for an existing PDB5 database.
    pub fn create_pdb6(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref();
        if path.exists() {
            return Err(DbError::Storage("refusing to create PDB6 over an existing database file".into()));
        }
        for suffix in [".wal", ".lock", ".pdb6.pages", ".pdb6.wal", ".pdb6.lock"] {
            let mut sidecar = path.as_os_str().to_os_string();
            sidecar.push(suffix);
            if PathBuf::from(sidecar).exists() {
                return Err(DbError::Storage(format!(
                    "refusing to create PDB6 over existing sidecar {suffix}"
                )));
            }
        }
        let mut database = Self::open(path)?;
        database.migrate_to_pdb6()?;
        Ok(database)
    }

    /// Open a persistent database. A missing file creates a new legacy PDB5 database.
    /// Use create_pdb6 for new page-backed databases; existing PDB5 files remain readable.
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
        let pdb6_state = load_pdb6_database(&path)?;
        let mut db = if let Some((tables, generation)) = pdb6_state {
            let mut database = Self::new();
            database.tables = tables;
            database.wal_sequence = generation;
            database.revision = generation;
            database.pdb6_mode = true;
            database
        } else if path.exists() {
            Self::decode_file(&path)?
        } else {
            Self::new()
        };
        db.storage_path = Some(path.clone());
        db.wal_path = Some(wal_file_path(&path));
        db._lock_file = Some(std::sync::Arc::new(lock_file));
        if !db.pdb6_mode {
            if !path.exists() {
                db.write_snapshot()?;
            }
            db.replay_wal()?;
            recover_page_tree(&path)?;
            db.ember_indexes_valid = db.sync_ember_indexes()?;
        }
        Ok(db)
    }

    /// Persist a snapshot and safely checkpoint all WAL records.
    /// The snapshot records the last applied WAL sequence, so a crash between
    /// the snapshot rename and WAL truncation cannot replay committed writes twice.
    pub fn save(&self) -> Result<(), DbError> {
        // PDB6 commits publish their own durable page-tree root. Never rewrite
        // the retained PDB5 compatibility snapshot from a PDB6-backed handle.
        if self.pdb6_mode {
            return Ok(());
        }
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
            if table.next_row_id == 0
                || table.rows.iter().any(|row| row.row_id >= table.next_row_id)
            {
                return Err(DbError::Storage(format!("invalid next stable row identifier in table {name}")));
            }
            out.extend_from_slice(&table.next_row_id.to_le_bytes());
            for row in &table.rows {
                if row.row_id == 0 {
                    return Err(DbError::Storage(format!("zero stable row identifier in table {name}")));
                }
                out.extend_from_slice(&row.row_id.to_le_bytes());
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
        let (format_version, wal_sequence) = if header == b"PDB5" {
            (5, u64::from_le_bytes(input.take(8)?.try_into().unwrap()))
        } else if header == b"PDB4" {
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
            let persisted_next_row_id = if format_version >= 5 {
                u64::from_le_bytes(input.take(8)?.try_into().unwrap())
            } else {
                1
            };
            if persisted_next_row_id == 0 {
                return Err(DbError::Storage("zero next stable row identifier in database file".into()));
            }
            let mut rows = Vec::with_capacity(row_count);
            let mut row_positions = BTreeMap::new();
            let mut next_row_id = persisted_next_row_id;
            for row_position in 0..row_count {
                let row_id = if format_version >= 5 {
                    u64::from_le_bytes(input.take(8)?.try_into().unwrap())
                } else {
                    u64::try_from(row_position).ok().and_then(|n| n.checked_add(1))
                        .ok_or_else(|| DbError::Storage("legacy row count exceeds stable identifier space".into()))?
                };
                if row_id == 0 || row_positions.insert(row_id, row_position).is_some() {
                    return Err(DbError::Storage("zero or duplicate stable row identifier in database file".into()));
                }
                let minimum_next = row_id.checked_add(1)
                    .ok_or_else(|| DbError::Storage("stable row identifier space exhausted".into()))?;
                if format_version >= 5 && persisted_next_row_id < minimum_next {
                    return Err(DbError::Storage("next stable row identifier does not exceed stored row identifiers".into()));
                }
                next_row_id = next_row_id.max(minimum_next);
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
                rows.push(Row { row_id, values });
            }
            let persisted_row_count = rows.len();
            let mut table = Table {
                columns,
                rows,
                persisted_row_count,
                rows_loaded: true,
                next_row_id,
                row_positions,
                indexes: BTreeMap::new(),
            };
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
        Ok(Database { tables, storage_path: None, wal_path: None, _lock_file: None, wal_sequence, revision: wal_sequence, pdb6_mode: false, ember_indexes_valid: false, ember_index_journal: Vec::new(), ember_journal_supported: true })
    }
}


/// Opens PDB6 as the SQL authority when a complete manifest is present.
/// Legacy PDB5 remains untouched and readable until this explicit switch.
fn load_pdb6_database(path: &Path) -> Result<Option<(BTreeMap<String, Table>, u64)>, DbError> {
    let mut pages_path = path.as_os_str().to_os_string();
    pages_path.push(".pdb6.pages");
    if !PathBuf::from(pages_path).exists() {
        return Ok(None);
    }
    let mut store = Pdb6Store::open(path, 128)?;
    let Some(manifest) = store.manifest()? else {
        return Ok(None);
    };
    if manifest.table_count > MAX_ITEMS as u32
        || manifest.next_table_id <= u64::from(manifest.table_count)
    {
        return Err(DbError::Storage("PDB6 manifest counters are inconsistent".into()));
    }

    let mut tables = BTreeMap::new();
    for table_id in 1..=u64::from(manifest.table_count) {
        let key = pdb6::table_key(table_id)?;
        let bytes = store.get(&key)?
            .ok_or_else(|| DbError::Storage(format!("PDB6 manifest references missing table {table_id}")))?;
        let catalog = CatalogTable::decode(&bytes)?;
        if catalog.table_id != table_id {
            return Err(DbError::Storage("PDB6 catalog key and table ID disagree".into()));
        }
        let columns = catalog.columns.iter().map(|column| Column {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        }).collect::<Vec<_>>();
        let mut persisted_row_count = 0usize;
        let mut after = None;
        loop {
            let row_ids = store.scan_row_ids(table_id, after, 5_000)?;
            if row_ids.is_empty() { break; }
            for row_id in &row_ids {
                if *row_id == 0 || *row_id >= catalog.next_row_id {
                    return Err(DbError::Storage("PDB6 row ID is outside the table allocator range".into()));
                }
            }
            persisted_row_count = persisted_row_count.checked_add(row_ids.len())
                .ok_or_else(|| DbError::Storage("PDB6 table row count overflow".into()))?;
            after = row_ids.last().copied();
            if row_ids.len() < 5_000 { break; }
        }
        let mut table = Table {
            columns,
            rows: Vec::new(),
            persisted_row_count,
            rows_loaded: false,
            next_row_id: catalog.next_row_id,
            row_positions: BTreeMap::new(),
            indexes: BTreeMap::new(),
        };

        for index in &catalog.indexes {
            let names = index.columns.iter().map(|ordinal| {
                table.columns.get(*ordinal as usize)
                    .map(|column| column.name.clone())
                    .ok_or_else(|| DbError::Storage("PDB6 index references a missing column".into()))
            }).collect::<Result<Vec<_>, _>>()?;
            table.add_index_columns(index.name.clone(), &names, index.unique)?;
        }
        if tables.insert(catalog.name.clone(), table).is_some() {
            return Err(DbError::Storage("duplicate table name in PDB6 catalog".into()));
        }
    }
    Ok(Some((tables, manifest.generation)))
}

/// Streams validated PDB6 rows in stable row-ID order without materializing a table.
/// The visitor returns false to stop the scan early (for example, after LIMIT).
pub(super) fn scan_pdb6_table_rows<F>(
    path: &Path,
    table_name: &str,
    batch_size: usize,
    mut visitor: F,
) -> Result<(), DbError>
where
    F: FnMut(Row) -> Result<bool, DbError>,
{
    if batch_size == 0 || batch_size > 5_000 {
        return Err(DbError::Constraint("PDB6 SQL scan batch size must be in 1..=5000".into()));
    }
    let mut store = Pdb6Store::open(path, 128)?;
    let manifest = store.manifest()?
        .ok_or_else(|| DbError::Storage("PDB6 SQL manifest is missing".into()))?;
    let mut catalog = None;
    for table_id in 1..=u64::from(manifest.table_count) {
        let bytes = store.get(&pdb6::table_key(table_id)?)?
            .ok_or_else(|| DbError::Storage(format!("PDB6 manifest references missing table {table_id}")))?;
        let candidate = CatalogTable::decode(&bytes)?;
        if candidate.name == table_name {
            catalog = Some(candidate);
            break;
        }
    }
    let catalog = catalog.ok_or_else(|| DbError::TableNotFound(table_name.to_owned()))?;
    let columns = catalog.columns.iter().map(|column| Column {
        name: column.name.clone(),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
    }).collect::<Vec<_>>();
    let mut after = None;
    loop {
        let row_ids = store.scan_row_ids(catalog.table_id, after, batch_size)?;
        if row_ids.is_empty() { break; }
        for row_id in &row_ids {
            if *row_id == 0 || *row_id >= catalog.next_row_id {
                return Err(DbError::Storage("PDB6 row ID is outside the table allocator range".into()));
            }
            let payload = store.get_row_payload(catalog.table_id, *row_id)?
                .ok_or_else(|| DbError::Storage(format!(
                    "PDB6 row descriptor missing for table {}, row {row_id}", catalog.name
                )))?;
            let values = sql_rows::SqlRowStore::decode_values(&payload, columns.len())?;
            for (column, value) in columns.iter().zip(&values) {
                validate_value(column, value)?;
            }
            if !visitor(Row { row_id: *row_id, values })? {
                return Ok(());
            }
        }
        after = row_ids.last().copied();
        if row_ids.len() < batch_size { break; }
    }
    Ok(())
}

/// Hydrates one table only when a write needs a mutable row image.
pub(super) fn hydrate_pdb6_table(
    path: &Path,
    table_name: &str,
    table: &mut Table,
) -> Result<(), DbError> {
    if table.rows_loaded { return Ok(()); }
    let mut rows = Vec::with_capacity(table.persisted_row_count);
    scan_pdb6_table_rows(path, table_name, 512, |row| {
        rows.push(row);
        Ok(true)
    })?;
    if rows.len() != table.persisted_row_count {
        return Err(DbError::Storage(format!(
            "PDB6 table {table_name} row count changed during hydration: expected {}, found {}",
            table.persisted_row_count, rows.len()
        )));
    }
    table.rows = rows;
    table.rebuild_row_positions();
    table.rebuild_indexes();
    table.rows_loaded = true;
    Ok(())
}

/// Hydrates all tables for explicit maintenance operations that need a complete view.
pub(super) fn hydrate_pdb6_rows(
    path: &Path,
    tables: &mut BTreeMap<String, Table>,
) -> Result<(), DbError> {
    for (table_name, table) in tables.iter_mut() {
        hydrate_pdb6_table(path, table_name, table)?;
    }
    Ok(())
}

/// Persists one complete SQL transaction through a single PDB6 root publication.
/// Row payloads, catalog records, secondary-index deltas and the manifest all
/// become visible together; PDB5 WAL is not consulted in PDB6 mode.
pub(super) fn persist_pdb6_transition(
    path: &Path,
    previous: &BTreeMap<String, Table>,
    next: &BTreeMap<String, Table>,
    generation: u64,
    coordinator_txid: Option<u64>,
) -> Result<u64, DbError> {
    let mut store = Pdb6Store::open(path, 128)?;
    let mut manifest = store.manifest()?
        .ok_or_else(|| DbError::Storage("PDB6 SQL authority has no completion manifest".into()))?;

    let mut table_ids = BTreeMap::<String, u64>::new();
    for table_id in 1..=u64::from(manifest.table_count) {
        let bytes = store.get(&pdb6::table_key(table_id)?)?
            .ok_or_else(|| DbError::Storage(format!("PDB6 catalog entry {table_id} is missing")))?;
        let catalog = CatalogTable::decode(&bytes)?;
        if table_ids.insert(catalog.name, table_id).is_some() {
            return Err(DbError::Storage("duplicate table name in PDB6 catalog".into()));
        }
    }
    if previous.keys().any(|name| !next.contains_key(name)) {
        return Err(DbError::Unsupported("dropping SQL tables is not supported by the PDB6 transition writer".into()));
    }

    let mut row_upserts = Vec::<(u64, u64, Vec<u8>)>::new();
    let mut other_upserts = Vec::<(Vec<u8>, Vec<u8>)>::new();
    let mut removals = Vec::<Vec<u8>>::new();
    let mut next_table_id = manifest.next_table_id;
    let mut new_table_count = manifest.table_count;

    for (table_name, table) in next {
        let table_id = if let Some(id) = table_ids.get(table_name) {
            *id
        } else {
            let id = next_table_id;
            next_table_id = next_table_id.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 table ID space exhausted".into()))?;
            new_table_count = new_table_count.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 table count overflow".into()))?;
            table_ids.insert(table_name.clone(), id);
            id
        };
        let catalog_indexes = table.indexes.iter().map(|(name, index)| {
            let columns = index.column_indices.iter()
                .map(|column| u32::try_from(*column)
                    .map_err(|_| DbError::Constraint("PDB6 index column ordinal exceeds u32".into())))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CatalogIndex { name: name.clone(), columns, unique: index.unique })
        }).collect::<Result<Vec<_>, DbError>>()?;
        let catalog = CatalogTable {
            table_id,
            name: table_name.clone(),
            columns: table.columns.iter().map(|column| CatalogColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            }).collect(),
            indexes: catalog_indexes,
            next_row_id: table.next_row_id,
        };
        let catalog_key = pdb6::table_key(table_id)?;
        let catalog_bytes = catalog.encode()?;
        if store.get(&catalog_key)?.as_deref() != Some(catalog_bytes.as_slice()) {
            other_upserts.push((catalog_key, catalog_bytes));
        }

        let old_table = previous.get(table_name);
        let indexes_changed = old_table.is_none_or(|old| {
            old.columns.iter().map(|c| (&c.name, &c.data_type, c.nullable))
                .ne(table.columns.iter().map(|c| (&c.name, &c.data_type, c.nullable)))
            || old.indexes.iter().map(|(name, index)| (name, &index.column_indices, index.unique))
                .ne(table.indexes.iter().map(|(name, index)| (name, &index.column_indices, index.unique)))
        });

        if indexes_changed {
            if let Some(old) = old_table {
                for (index_offset, _) in old.indexes.values().enumerate() {
                    let index_id = u64::try_from(index_offset + 1)
                        .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                    let prefix = pdb6::index_prefix(table_id, index_id)?;
                    let mut cursor: Option<Vec<u8>> = None;
                    loop {
                        let page = store.scan_prefix_after(&prefix, cursor.as_deref(), 5_000)?;
                        if page.is_empty() { break; }
                        cursor = page.last().map(|(key, _)| key.clone());
                        removals.extend(page.into_iter().map(|(key, _)| key));
                        if removals.len() > 5_000 {
                            return Err(DbError::Constraint("PDB6 transaction exceeds the 5,000-mutation limit while rebuilding indexes".into()));
                        }
                    }
                }
            }
            for (index_offset, index) in table.indexes.values().enumerate() {
                let index_id = u64::try_from(index_offset + 1)
                    .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                for row in &table.rows {
                    let encoded = index_codec::encode_index_key(&IndexKey::from_columns(&row.values, &index.column_indices))?;
                    let mut key = pdb6::index_key(table_id, index_id, &encoded)?;
                    key.extend_from_slice(&row.row_id.to_be_bytes());
                    if key.len() > 1_024 {
                        return Err(DbError::Constraint("PDB6 index key exceeds 1,024 bytes".into()));
                    }
                    other_upserts.push((key, Vec::new()));
                }
            }
            for row in &table.rows {
                let payload = sql_rows::SqlRowStore::encode_values(&row.values, table.columns.len())?;
                row_upserts.push((table_id, row.row_id, payload));
            }
        } else {
            let old = old_table.expect("unchanged indexes require an old table");
            let old_rows = old.rows.iter().map(|row| (row.row_id, row))
                .collect::<BTreeMap<_, _>>();
            let new_rows = table.rows.iter().map(|row| (row.row_id, row))
                .collect::<BTreeMap<_, _>>();

            for (row_id, old_row) in &old_rows {
                let changed = new_rows.get(row_id).is_none_or(|new_row| old_row.values != new_row.values);
                if !changed { continue; }
                if new_rows.contains_key(row_id) {
                    // Replacing a row keeps its stable ID but changes its payload.
                } else {
                    removals.push(pdb6::row_key(table_id, *row_id)?);
                }
                for (index_offset, index) in old.indexes.values().enumerate() {
                    let index_id = u64::try_from(index_offset + 1)
                        .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                    let encoded = index_codec::encode_index_key(&IndexKey::from_columns(&old_row.values, &index.column_indices))?;
                    let mut key = pdb6::index_key(table_id, index_id, &encoded)?;
                    key.extend_from_slice(&row_id.to_be_bytes());
                    removals.push(key);
                }
            }
            for (row_id, new_row) in &new_rows {
                let changed = old_rows.get(row_id).is_none_or(|old_row| old_row.values != new_row.values);
                if !changed { continue; }
                let payload = sql_rows::SqlRowStore::encode_values(&new_row.values, table.columns.len())?;
                row_upserts.push((table_id, *row_id, payload));
                for (index_offset, index) in table.indexes.values().enumerate() {
                    let index_id = u64::try_from(index_offset + 1)
                        .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                    let encoded = index_codec::encode_index_key(&IndexKey::from_columns(&new_row.values, &index.column_indices))?;
                    let mut key = pdb6::index_key(table_id, index_id, &encoded)?;
                    key.extend_from_slice(&row_id.to_be_bytes());
                    if key.len() > 1_024 {
                        return Err(DbError::Constraint("PDB6 index key exceeds 1,024 bytes".into()));
                    }
                    other_upserts.push((key, Vec::new()));
                }
            }
        }
    }

    if let Some(transaction_id) = coordinator_txid {
        other_upserts.push((
            pdb6::coordinator_tx_key(transaction_id),
            transaction_id.to_le_bytes().to_vec(),
        ));
    }
    if row_upserts.is_empty() && other_upserts.is_empty() && removals.is_empty() {
        return Ok(manifest.generation);
    }

    manifest.generation = generation;
    manifest.last_tx_id = manifest.last_tx_id.checked_add(1)
        .ok_or_else(|| DbError::Storage("PDB6 transaction ID space exhausted".into()))?;
    manifest.next_table_id = next_table_id;
    manifest.table_count = new_table_count;
    other_upserts.push((pdb6::database_root_key(), manifest.encode()?));

    store.apply_row_batch(row_upserts, other_upserts, removals)?;
    Ok(generation)
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
    fn legacy_pdb4_rows_receive_stable_identifiers_on_load() {
        let path = test_path("legacy-pdb4-row-ids");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"PDB4");
        bytes.extend_from_slice(&0u64.to_le_bytes());
        put_u32(&mut bytes, 1);
        put_string(&mut bytes, "legacy_rows").unwrap();
        put_u32(&mut bytes, 1);
        put_string(&mut bytes, "value").unwrap();
        bytes.push(2); // TEXT
        bytes.push(0); // NOT NULL
        put_u32(&mut bytes, 0); // indexes
        put_u32(&mut bytes, 2); // rows
        bytes.push(2);
        put_string(&mut bytes, "first").unwrap();
        bytes.push(2);
        put_string(&mut bytes, "second").unwrap();
        fs::write(&path, bytes).unwrap();

        let db = Database::decode_file(&path).unwrap();
        let table = db.tables.get("legacy_rows").unwrap();
        assert_eq!(table.rows[0].row_id, 1);
        assert_eq!(table.rows[1].row_id, 2);
        assert_eq!(table.next_row_id, 3);
        assert_eq!(table.row_positions.get(&1), Some(&0));
        assert_eq!(table.row_positions.get(&2), Some(&1));
        drop(db);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn explicit_pdb5_to_pdb6_migration_copies_catalog_rows_and_indexes_without_touching_source() {
        let path = test_path("pdb6-explicit-migration");
        let pages_path = PathBuf::from(format!("{}.pdb6.pages", path.display()));
        let wal_path = PathBuf::from(format!("{}.pdb6.wal", path.display()));
        let lock_path = PathBuf::from(format!("{}.pdb6.lock", path.display()));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE players (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("CREATE INDEX idx_players_uuid ON players (uuid)").unwrap();
            db.insert_many("players", (1..=3).map(|id| vec![
                Value::Text(format!("player-{id}")),
                Value::Integer(id),
            ])).unwrap();

            let source_snapshot = fs::read(&path).unwrap();
            let source_wal = fs::read(wal_file_path(&path)).unwrap();
            db.migrate_to_pdb6().unwrap();
            assert_eq!(fs::read(&path).unwrap(), source_snapshot);
            assert_eq!(fs::read(wal_file_path(&path)).unwrap(), source_wal);

            let mut store = Pdb6Store::open(&path, 32).unwrap();
            let manifest = store.manifest().unwrap().unwrap();
            assert_eq!(manifest.table_count, 1);
            assert_eq!(manifest.next_table_id, 2);
            assert_eq!(manifest.generation, db.wal_sequence);

            let catalog = pdb6::CatalogTable::decode(
                &store.get(&pdb6::table_key(1).unwrap()).unwrap().unwrap()
            ).unwrap();
            assert_eq!(catalog.name, "players");
            assert_eq!(catalog.columns.len(), 2);
            assert_eq!(catalog.indexes.len(), 1);
            assert_eq!(catalog.indexes[0].name, "idx_players_uuid");

            let row = store.get_row_payload(1, 2).unwrap().unwrap();
            assert_eq!(
                sql_rows::SqlRowStore::decode_values(&row, 2).unwrap(),
                vec![Value::Text("player-2".into()), Value::Integer(2)]
            );
            assert_eq!(store.scan_row_ids(1, None, 10).unwrap(), vec![1, 2, 3]);
            let index_prefix = pdb6::index_prefix(1, 1).unwrap();
            let index_entries = store.scan_prefix(&index_prefix, 10).unwrap();
            assert_eq!(index_entries.len(), 3);
            assert!(index_entries.iter().all(|(key, _)| key.len() > index_prefix.len()));
            drop(store);

            // PDB5 remains the live SQL source until a separately verified cutover.
            let result = db.execute("SELECT * FROM players").unwrap();
            assert_eq!(result.rows.len(), 3);
            assert!(db.migrate_to_pdb6().is_err());
        }
        for candidate in [
            &path, &pages_path, &wal_path, &lock_path,
            &wal_file_path(&path), &lock_file_path(&path),
        ] {
            let _ = fs::remove_file(candidate);
        }
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
            db.insert_many("players", (0..700).map(|coins| vec![
                Value::Text("player-a".into()),
                Value::Integer(coins),
            ])).unwrap();
        }

        let mut reopened = Database::open(&path).unwrap();
        let query = reopened.execute("SELECT * FROM players WHERE uuid = 'player-a'").unwrap();
        assert_eq!(query.rows.len(), 700);
        let limited = reopened.execute("SELECT * FROM players WHERE uuid = 'player-a' LIMIT 5").unwrap();
        assert_eq!(limited.rows.len(), 5);
        assert_eq!(limited.rows[0].values[1], Value::Integer(0));
        assert_eq!(limited.rows[4].values[1], Value::Integer(4));
        let empty = reopened.execute("SELECT * FROM players WHERE uuid = 'player-a' LIMIT 0").unwrap();
        assert!(empty.rows.is_empty());
        drop(reopened);

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
        assert_eq!(refs.len(), 700);
        assert_eq!(refs.first(), Some(&1));
        assert_eq!(refs.last(), Some(&700));
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

        let mut reopened = Database::open(&path).unwrap();
        let query = reopened.execute("SELECT * FROM rebuild_rows WHERE label = 'durable'").unwrap();
        assert_eq!(query.rows.len(), 1);
        assert_eq!(query.rows[0].values[0], Value::Integer(7));
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
        assert_eq!(refs, vec![1]);
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

/// Explicitly copies the currently opened PDB5 database into the PDB6
/// catalog/row/index keyspaces. The PDB5 snapshot and WAL remain untouched and
/// authoritative; this operation is restartable at the logical-key level and
/// only publishes the PDB6 manifest after every table, row, and index is copied.
///
/// This is intentionally not an automatic migration: callers must verify the
/// PDB6 copy before switching the database's authoritative storage format.
impl Database {
    /// Reclaims obsolete persistent B+ tree node pages in a PDB6 database.
    /// Overflow row pages and unrecognized page types are retained conservatively.
    /// PDB6 currently has no retained-snapshot API; this operation relies on the
    /// store's exclusive process lock and must not be treated as snapshot vacuum.
    pub fn vacuum_pdb6(&mut self) -> Result<u64, DbError> {
        if !self.pdb6_mode {
            return Err(DbError::Storage("PDB6 vacuum requires a database migrated to PDB6".into()));
        }
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 vacuum requires a persistent database".into()))?;
        let mut store = Pdb6Store::open(path, 128)?;
        store.vacuum_unreachable_tree_pages()
    }

    /// Creates a durable named snapshot of the current PDB6 root.
    pub fn create_pdb6_snapshot(&mut self, name: &str) -> Result<(), DbError> {
        if !self.pdb6_mode { return Err(DbError::Storage("PDB6 snapshots require a PDB6 database".into())); }
        let path = self.storage_path.as_deref().ok_or_else(|| DbError::Storage("PDB6 snapshots require a persistent database".into()))?;
        Pdb6Store::open(path, 128)?.create_snapshot(name)
    }

    /// Deletes a durable named snapshot. A later vacuum can reclaim pages no longer referenced.
    pub fn delete_pdb6_snapshot(&mut self, name: &str) -> Result<bool, DbError> {
        if !self.pdb6_mode { return Err(DbError::Storage("PDB6 snapshots require a PDB6 database".into())); }
        let path = self.storage_path.as_deref().ok_or_else(|| DbError::Storage("PDB6 snapshots require a persistent database".into()))?;
        Pdb6Store::open(path, 128)?.delete_snapshot(name)
    }

    /// Lists durable named PDB6 snapshots in lexical order.
    pub fn list_pdb6_snapshots(&mut self) -> Result<Vec<String>, DbError> {
        if !self.pdb6_mode { return Err(DbError::Storage("PDB6 snapshots require a PDB6 database".into())); }
        let path = self.storage_path.as_deref().ok_or_else(|| DbError::Storage("PDB6 snapshots require a persistent database".into()))?;
        Pdb6Store::open(path, 128)?.list_snapshots()
    }

    pub fn migrate_to_pdb6(&mut self) -> Result<(), DbError> {
        let database_path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 migration requires a persistent database opened with Database::open".into()))?;
        let mut store = Pdb6Store::open(database_path, 128)?;
        if store.manifest()?.is_some() {
            return Err(DbError::Storage(
                "PDB6 manifest already exists; refusing to overwrite an existing PDB6 database".into()
            ));
        }

        let table_count = u32::try_from(self.tables.len())
            .map_err(|_| DbError::Constraint("PDB6 migration table count exceeds u32".into()))?;
        for table_id in 1..=u64::from(table_count) {
            if store.get(&pdb6::table_key(table_id)?)?.is_some() {
                return Err(DbError::Storage(
                    "an incomplete or existing PDB6 catalog is present; remove its .pdb6.pages, .pdb6.wal, and .pdb6.lock sidecars before restarting migration".into()
                ));
            }
        }
        let next_table_id = u64::from(table_count).checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 table ID space exhausted".into()))?;

        for (table_offset, (table_name, table)) in self.tables.iter().enumerate() {
            let table_id = u64::try_from(table_offset + 1)
                .map_err(|_| DbError::Storage("PDB6 table ID space exhausted".into()))?;
            let catalog_indexes = table.indexes.iter().map(|(name, index)| {
                let columns = index.column_indices.iter()
                    .map(|column| u32::try_from(*column)
                        .map_err(|_| DbError::Constraint("PDB6 index column ordinal exceeds u32".into())))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(CatalogIndex { name: name.clone(), columns, unique: index.unique })
            }).collect::<Result<Vec<_>, DbError>>()?;
            let catalog = CatalogTable {
                table_id,
                name: table_name.clone(),
                columns: table.columns.iter().map(|column| CatalogColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                }).collect(),
                indexes: catalog_indexes,
                next_row_id: table.next_row_id,
            };
            let catalog_key = pdb6::table_key(table_id)?;
            let encoded_catalog = catalog.encode()?;
            store.apply_batch(
                [(catalog_key.clone(), encoded_catalog)],
                std::iter::empty::<Vec<u8>>(),
            )?;
            let stored_catalog = store.get(&catalog_key)?
                .ok_or_else(|| DbError::Storage(format!("PDB6 migration lost catalog for table {table_name}")))?;
            if CatalogTable::decode(&stored_catalog)? != catalog {
                return Err(DbError::Storage(format!("PDB6 migration catalog verification failed for table {table_name}")));
            }

            for chunk in table.rows.chunks(5_000) {
                let encoded_rows = chunk.iter().map(|row| {
                    Ok((table_id, row.row_id, sql_rows::SqlRowStore::encode_values(&row.values, table.columns.len())?))
                }).collect::<Result<Vec<_>, DbError>>()?;
                if !encoded_rows.is_empty() {
                    store.apply_row_batch(
                        encoded_rows.clone(),
                        std::iter::empty::<(Vec<u8>, Vec<u8>)>(),
                        std::iter::empty::<Vec<u8>>(),
                    )?;
                    for (stored_table_id, row_id, expected_payload) in encoded_rows {
                        let actual_payload = store.get_row_payload(stored_table_id, row_id)?
                            .ok_or_else(|| DbError::Storage(format!(
                                "PDB6 migration lost row {row_id} in table {table_name}"
                            )))?;
                        if actual_payload != expected_payload {
                            return Err(DbError::Storage(format!(
                                "PDB6 migration row verification failed for table {table_name}, row {row_id}"
                            )));
                        }
                    }
                }
            }

            // Rebuild index entries from stable row IDs, not vector positions.
            // Flush bounded chunks so migration memory stays independent of index size.
            for (index_offset, index) in table.indexes.values().enumerate() {
                let index_id = u64::try_from(index_offset + 1)
                    .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                let mut pending = Vec::with_capacity(5_000);
                for row in &table.rows {
                    let key = IndexKey::from_columns(&row.values, &index.column_indices);
                    let encoded_key = index_codec::encode_index_key(&key)?;
                    let mut disk_key = pdb6::index_key(table_id, index_id, &encoded_key)?;
                    disk_key.extend_from_slice(&row.row_id.to_be_bytes());
                    if disk_key.len() > 1024 {
                        return Err(DbError::Constraint(format!(
                            "index key for table {table_name} exceeds the PDB6 key limit"
                        )));
                    }
                    pending.push((disk_key, Vec::new()));
                    if pending.len() == 5_000 {
                        let batch = std::mem::take(&mut pending);
                        let keys = batch.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                        store.apply_batch(batch, std::iter::empty::<Vec<u8>>())?;
                        for key in keys {
                            if store.get(&key)?.is_none() {
                                return Err(DbError::Storage(format!(
                                    "PDB6 migration index verification failed for table {table_name}"
                                )));
                            }
                        }
                        pending = Vec::with_capacity(5_000);
                    }
                }
                if !pending.is_empty() {
                    let keys = pending.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                    store.apply_batch(pending, std::iter::empty::<Vec<u8>>())?;
                    for key in keys {
                        if store.get(&key)?.is_none() {
                            return Err(DbError::Storage(format!(
                                "PDB6 migration index verification failed for table {table_name}"
                            )));
                        }
                    }
                }
            }
        }

        // Verify the catalog cardinality before publishing the completion marker.
        let stored_table_keys = store.scan_prefix(&[pdb6::KEY_TABLE], table_count as usize)?;
        if stored_table_keys.len() != table_count as usize {
            return Err(DbError::Storage("PDB6 migration table-count verification failed".into()));
        }

        // The manifest is the completion marker and is published last. If a
        // process stops earlier, PDB5 is still untouched and migration can be
        // restarted after the incomplete PDB6 sidecar is removed by the operator.
        store.set_manifest(DatabaseManifest {
            generation: self.wal_sequence,
            last_tx_id: 0,
            next_table_id,
            table_count,
        })?;
        // The just-published manifest makes PDB6 authoritative immediately for
        // this handle too; subsequent writes must not continue into the PDB5 WAL.
        self.pdb6_mode = true;
        self.ember_indexes_valid = false;
        Ok(())
    }
}

/// Rebuild the durable EmberTree index mirror from the SQL-WAL-recovered state.
/// SQL snapshots/WAL remain authoritative; this operation is idempotent and repairable.
impl Database {
    pub(crate) fn sync_ember_indexes_journal(
        &self,
        journal: &[super::EmberIndexDelta],
        supported: bool,
        previous_mirror_valid: bool,
    ) -> Result<bool, DbError> {
        if !previous_mirror_valid {
            return self.sync_ember_indexes();
        }
        if !supported {
            return Ok(false);
        }
        let Some(database_path) = self.storage_path.as_deref() else { return Ok(false); };
        if journal.is_empty() {
            return Ok(true);
        }
        // Collapse repeated mutations by key in transaction order. The final
        // operation wins, so INSERT followed by DELETE cannot resurrect a stale
        // persistent index entry when removals and upserts are batched separately.
        let mut final_states = BTreeMap::<Vec<u8>, bool>::new();
        for delta in journal {
            final_states.insert(delta.key.clone(), delta.insert);
        }
        let removals = final_states.iter().filter(|(_, insert)| !**insert)
            .map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let upserts = final_states.iter().filter(|(_, insert)| **insert)
            .map(|(key, _)| (key.clone(), Vec::new())).collect::<Vec<_>>();
        let page_path = page_tree_file_path(database_path);
        let tree_wal_path = page_tree_wal_file_path(database_path);
        if !page_path.exists() {
            // A removed derived sidecar cannot be repaired by applying only the
            // latest transaction journal; rebuild it from canonical SQL state.
            return self.sync_ember_indexes();
        }
        let mut store = extent::ExtentFile::open(&page_path)?;
        page_tree::apply_mutations(&mut store, &tree_wal_path, &removals, &upserts)?;
        Ok(true)
    }

    pub(crate) fn sync_ember_indexes(&self) -> Result<bool, DbError> {
        let Some(database_path) = self.storage_path.as_deref() else { return Ok(false); };
        let page_path = page_tree_file_path(database_path);
        let tree_wal_path = page_tree_wal_file_path(database_path);
        let Some(entries) = collect_ember_index_entries(&self.tables)? else { return Ok(false); };

        if entries.is_empty() && !page_path.exists() && !tree_wal_path.exists() {
            return Ok(true);
        }
        let mut store = extent::ExtentFile::open(&page_path)?;
        if entries.is_empty() {
            store.publish_root(None)?;
            return Ok(true);
        }
        let encoded = entries.into_iter().map(|key| (key, Vec::new())).collect::<Vec<_>>();
        page_tree::replace_all(&mut store, &tree_wal_path, &encoded)?;
        Ok(true)
    }

}

fn collect_ember_index_entries(
    tables: &BTreeMap<String, Table>,
) -> Result<Option<BTreeSet<Vec<u8>>>, DbError> {
    let mut entries = BTreeSet::<Vec<u8>>::new();
    for (table_name, table) in tables {
        for (index_name, index) in &table.indexes {
            let upper = super::IndexKey::Composite(vec![super::IndexAtom::Boolean(true); 256]);
            for (key, rows) in index.entries.scan_range(&super::IndexKey::Null, &upper) {
                let encoded = match index_codec::encode_index_key(&key) {
                    Ok(encoded) => encoded,
                    Err(_) => return Ok(None),
                };
                let mut disk_key = Vec::with_capacity(4 + table_name.len() + index_name.len() + encoded.len());
                put_namespace_part(&mut disk_key, table_name.as_bytes())?;
                put_namespace_part(&mut disk_key, index_name.as_bytes())?;
                disk_key.extend_from_slice(&encoded);
                if disk_key.len() > 1015 {
                    return Ok(None);
                }
                for row_id in rows {
                    if row_id == 0 {
                        return Err(DbError::Storage("in-memory SQL index contains a zero stable row identifier".into()));
                    }
                    let row_position = table.row_positions.get(&row_id).copied()
                        .ok_or_else(|| DbError::Storage("in-memory SQL index references an unknown stable row identifier".into()))?;
                    let row = table.rows.get(row_position)
                        .ok_or_else(|| DbError::Storage("stable row identifier maps to an invalid row position".into()))?;
                    if row.row_id != row_id {
                        return Err(DbError::Storage("stable row identifier map does not match the table row".into()));
                    }
                    let mut entry_key = disk_key.clone();
                    entry_key.push(0);
                    entry_key.extend_from_slice(&row_id.to_be_bytes());
                    entries.insert(entry_key);
                }
            }
        }
    }
    Ok(Some(entries))
}

pub(crate) fn encode_ember_index_entry(
    table_name: &str,
    index_name: &str,
    key: &super::IndexKey,
    row_id: u64,
) -> Result<Option<Vec<u8>>, DbError> {
    if row_id == 0 {
        return Err(DbError::Storage("cannot encode a zero stable row identifier".into()));
    }
    let encoded = match index_codec::encode_index_key(key) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let mut out = Vec::with_capacity(4 + table_name.len() + index_name.len() + encoded.len() + 9);
    put_namespace_part(&mut out, table_name.as_bytes())?;
    put_namespace_part(&mut out, index_name.as_bytes())?;
    out.extend_from_slice(&encoded);
    if out.len() > 1015 {
        return Ok(None);
    }
    out.push(0);
    out.extend_from_slice(&row_id.to_be_bytes());
    Ok(Some(out))
}

fn put_namespace_part(out: &mut Vec<u8>, value: &[u8]) -> Result<(), DbError> {
    let length = u16::try_from(value.len())
        .map_err(|_| DbError::Storage("SQL table or index name exceeds 65535 bytes".into()))?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

/// Look up all row references for one exact SQL index key in the persistent tree.
pub(crate) fn lookup_ember_index(
    database_path: &Path,
    table_name: &str,
    index_name: &str,
    key: &super::IndexKey,
    limit: Option<usize>,
) -> Result<Option<Vec<u64>>, DbError> {
    let page_path = page_tree_file_path(database_path);
    if !page_path.exists() {
        return Ok(None);
    }
    let mut prefix = Vec::new();
    put_namespace_part(&mut prefix, table_name.as_bytes())?;
    put_namespace_part(&mut prefix, index_name.as_bytes())?;
    prefix.extend_from_slice(&index_codec::encode_index_key(key)?);
    if prefix.len() > 1015 {
        return Err(DbError::Storage("namespaced SQL index key leaves no room for its row-reference suffix".into()));
    }
    let mut lower = prefix.clone();
    lower.push(0);
    lower.extend_from_slice(&0u64.to_be_bytes());
    let mut upper = prefix;
    upper.push(0);
    upper.extend_from_slice(&u64::MAX.to_be_bytes());

    if limit == Some(0) {
        return Ok(Some(Vec::new()));
    }
    let mut store = extent::ExtentFile::open(&page_path)?;
    let entries = page_tree::scan_range_limited(&mut store, &lower, &upper, limit)?;
    let mut rows = Vec::with_capacity(entries.len());
    for (entry_key, _) in entries {
        if entry_key.len() != lower.len() {
            return Err(DbError::Storage("malformed persistent SQL index row-reference key".into()));
        }
        let row_id = u64::from_be_bytes(entry_key[entry_key.len() - 8..].try_into().unwrap());
        if row_id == 0 {
            return Err(DbError::Storage("persistent SQL index contains a zero stable row identifier".into()));
        }
        rows.push(row_id);
    }
    Ok(Some(rows))
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
    page_tree::recover(&mut store, &wal_path)?;
    page_tree::validate_tree(&mut store)
}

fn lock_file_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".lock");
    PathBuf::from(name)
}
