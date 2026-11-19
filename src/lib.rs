//! PostemberDB: an independent, Rust-native SQL-like database engine.
//!
//! The engine supports in-memory and persistent SQL databases, transaction WAL,
//! a derived persistent SQL-index mirror, plus independent native KV and Registry sidecars.

use std::collections::BTreeMap;
use crate::storage::btree::BPlusTree;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

mod storage;
mod sql;
mod kv;
pub mod protocol;
pub mod auth;
pub mod version;

pub use kv::{KeyValueStore, Registry, RegistryValue};
pub use auth::{AuthStore, Permission, Permissions, Principal};
pub use storage::record_store::RecordStore;
pub use storage::sql_rows::SqlRowStore;
pub use storage::transaction_coordinator::{CoordinatorParticipant, RecoveredTransaction, TransactionCoordinator, TransactionState};
pub use storage::emberforge7::{EmberForge7Namespace, EmberForge7NamespaceKind, EmberForge7PlayerProfile, EmberForge7Stats, EmberForge7Store};

use sql::{
    apply_upsert, bind_parameters, find_kw, normalize_ident, parse_equality_predicates,
    parse_value, split_csv, starts_with_kw, take_parenthesized, value_to_sql,
    validate_value,
};

/// Runtime statistics for the bounded fixed-page buffer pool.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageCacheStats {
    /// Maximum number of decoded page payloads retained by this file handle.
    pub capacity_pages: usize,
    /// Number of decoded page payloads currently resident.
    pub resident_pages: usize,
    /// Total bytes held by cached page payloads.
    pub resident_bytes: usize,
    /// Reads served directly from the cache.
    pub hits: u64,
    /// Reads that required loading and validating a page from the file.
    pub misses: u64,
    /// Cached pages removed to make room for newer pages or a smaller capacity.
    pub evictions: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Text(v) => write!(f, "{v}"),
            Value::Boolean(v) => write!(f, "{v}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Row {
    /// Stable table-local identifier, independent of the current vector position.
    pub row_id: u64,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub affected_rows: usize,
    pub message: String,
}

impl QueryResult {
    fn message(message: impl Into<String>) -> Self {
        Self { columns: vec![], rows: vec![], affected_rows: 0, message: message.into() }
    }
}

#[derive(Debug)]
pub enum DbError {
    Parse(String),
    TableExists(String),
    TableNotFound(String),
    ColumnNotFound(String),
    TypeMismatch { column: String, expected: DataType },
    Constraint(String),
    Unsupported(String),
    Storage(String),
    TransactionConflict,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Parse(s) => write!(f, "SQL parse error: {s}"),
            DbError::TableExists(s) => write!(f, "table already exists: {s}"),
            DbError::TableNotFound(s) => write!(f, "table not found: {s}"),
            DbError::ColumnNotFound(s) => write!(f, "column not found: {s}"),
            DbError::TypeMismatch { column, expected } => write!(f, "type mismatch for {column}; expected {expected:?}"),
            DbError::Constraint(s) => write!(f, "constraint violation: {s}"),
            DbError::Unsupported(s) => write!(f, "unsupported SQL: {s}"),
            DbError::Storage(s) => write!(f, "storage error: {s}"),
            DbError::TransactionConflict => write!(f, "transaction conflict: database changed since the session began"),
        }
    }
}

impl std::error::Error for DbError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexAtom {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl From<&Value> for IndexAtom {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(value) => Self::Integer(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Boolean(value) => Self::Boolean(*value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKey {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
    Composite(Vec<IndexAtom>),
}

impl From<&Value> for IndexKey {
    fn from(value: &Value) -> Self {
        match IndexAtom::from(value) {
            IndexAtom::Null => Self::Null,
            IndexAtom::Integer(value) => Self::Integer(value),
            IndexAtom::Text(value) => Self::Text(value),
            IndexAtom::Boolean(value) => Self::Boolean(value),
        }
    }
}

impl IndexKey {
    fn from_columns(values: &[Value], columns: &[usize]) -> Self {
        if columns.len() == 1 {
            return Self::from(&values[columns[0]]);
        }
        Self::Composite(columns.iter().map(|index| IndexAtom::from(&values[*index])).collect())
    }
}

#[derive(Debug, Clone)]
struct Index {
    column_indices: Vec<usize>,
    unique: bool,
    /// Stable table-local row identifiers; never vector positions.
    entries: BPlusTree<IndexKey, Vec<u64>>,
}

#[derive(Debug, Clone)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>,
    next_row_id: u64,
    row_positions: BTreeMap<u64, usize>,
    indexes: BTreeMap<String, Index>,
}

impl Table {
    fn add_index(&mut self, index_name: String, column_name: &str) -> Result<(), DbError> {
        self.add_index_columns(index_name, &[column_name.to_owned()], false)
    }

    fn add_index_columns(&mut self, index_name: String, column_names: &[String], unique: bool) -> Result<(), DbError> {
        if self.indexes.contains_key(&index_name) {
            return Err(DbError::Constraint(format!("index already exists: {index_name}")));
        }
        if column_names.is_empty() {
            return Err(DbError::Constraint("an index must contain at least one column".into()));
        }
        let column_indices = column_names.iter().map(|name| {
            self.columns.iter().position(|column| column.name == normalize_ident(name))
                .ok_or_else(|| DbError::ColumnNotFound(name.clone()))
        }).collect::<Result<Vec<_>, _>>()?;
        let mut index = Index { column_indices, unique, entries: BPlusTree::new() };
        for row in &self.rows {
            let key = IndexKey::from_columns(&row.values, &index.column_indices);
            let mut row_ids = index.entries.get(&key).cloned().unwrap_or_default();
            if unique && !row_ids.is_empty() {
                return Err(DbError::Constraint(format!("duplicate key while building unique index {index_name}")));
            }
            row_ids.push(row.row_id);
            index.entries.insert(key, row_ids);
        }
        self.indexes.insert(index_name, index);
        Ok(())
    }

    fn validate_unique(&self, values: &[Value]) -> Result<(), DbError> {
        for (name, index) in &self.indexes {
            if index.unique {
                let key = IndexKey::from_columns(values, &index.column_indices);
                if index.entries.contains_key(&key) {
                    return Err(DbError::Constraint(format!("duplicate key for unique index {name}")));
                }
            }
        }
        Ok(())
    }

    /// Validates a single-column UPDATE against every unique index it changes.
    /// All rows in the statement are considered together, so a bulk update
    /// cannot introduce duplicates among the rows being changed.
    fn validate_unique_update(
        &self,
        row_positions: &[usize],
        target_column: usize,
        new_value: &Value,
    ) -> Result<(), DbError> {
        let changing_ids = row_positions.iter()
            .filter_map(|position| self.rows.get(*position).map(|row| row.row_id))
            .collect::<std::collections::BTreeSet<_>>();

        for (name, index) in &self.indexes {
            if !index.unique || !index.column_indices.contains(&target_column) {
                continue;
            }

            let mut seen = std::collections::BTreeSet::new();
            for row in &self.rows {
                if changing_ids.contains(&row.row_id) {
                    continue;
                }
                seen.insert(IndexKey::from_columns(&row.values, &index.column_indices));
            }

            for position in row_positions {
                let Some(row) = self.rows.get(*position) else { continue; };
                let mut values = row.values.clone();
                values[target_column] = new_value.clone();
                let key = IndexKey::from_columns(&values, &index.column_indices);
                if !seen.insert(key) {
                    return Err(DbError::Constraint(format!("duplicate key for unique index {name}")));
                }
            }
        }
        Ok(())
    }

    fn insert_row_id(index: &mut Index, key: IndexKey, row_id: u64) {
        let mut row_ids = index.entries.get(&key).cloned().unwrap_or_default();
        match row_ids.binary_search(&row_id) {
            Ok(_) => return,
            Err(position) => row_ids.insert(position, row_id),
        }
        index.entries.insert(key, row_ids);
    }

    fn remove_row_id(index: &mut Index, key: &IndexKey, row_id: u64) {
        let Some(mut row_ids) = index.entries.get(key).cloned() else { return; };
        if let Ok(position) = row_ids.binary_search(&row_id) {
            row_ids.remove(position);
        } else {
            return;
        }
        if row_ids.is_empty() {
            index.entries.remove(key);
        } else {
            index.entries.insert(key.clone(), row_ids);
        }
    }

    fn index_row(&mut self, row_index: usize) {
        let row = &self.rows[row_index];
        let row_id = row.row_id;
        for index in self.indexes.values_mut() {
            let key = IndexKey::from_columns(&row.values, &index.column_indices);
            Self::insert_row_id(index, key, row_id);
        }
    }

    fn remove_index_row(&mut self, row: &Row) {
        for index in self.indexes.values_mut() {
            let key = IndexKey::from_columns(&row.values, &index.column_indices);
            Self::remove_row_id(index, &key, row.row_id);
        }
    }

    /// Updates only index trees whose key actually changes for this row.
    fn reindex_row(&mut self, row_index: usize, old_values: &[Value]) {
        let row = &self.rows[row_index];
        let row_id = row.row_id;
        for index in self.indexes.values_mut() {
            let old_key = IndexKey::from_columns(old_values, &index.column_indices);
            let new_key = IndexKey::from_columns(&row.values, &index.column_indices);
            if old_key == new_key {
                continue;
            }
            Self::remove_row_id(index, &old_key, row_id);
            Self::insert_row_id(index, new_key, row_id);
        }
    }

    fn rebuild_row_positions(&mut self) {
        self.row_positions.clear();
        for (position, row) in self.rows.iter().enumerate() {
            self.row_positions.insert(row.row_id, position);
        }
    }

    fn rebuild_indexes(&mut self) {
        for index in self.indexes.values_mut() {
            index.entries.clear();
            for row in &self.rows {
                let key = IndexKey::from_columns(&row.values, &index.column_indices);
                let mut row_ids = index.entries.get(&key).cloned().unwrap_or_default();
                row_ids.push(row.row_id);
                index.entries.insert(key, row_ids);
            }
        }
    }

    fn positions_for_row_ids(&self, row_ids: &[u64]) -> Vec<usize> {
        row_ids.iter()
            .filter_map(|row_id| self.row_positions.get(row_id).copied())
            .collect()
    }

    fn matching_rows_for_predicates(&self, predicates: &[(usize, Value)]) -> Vec<usize> {
        let columns = predicates.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        if let Some(index) = self.indexes.values().find(|index| index.column_indices == columns) {
            let values = predicates.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>();
            let key = IndexKey::from_columns(&values, &(0..values.len()).collect::<Vec<_>>());
            return index.entries.get(&key)
                .map(|row_ids| self.positions_for_row_ids(row_ids))
                .unwrap_or_default();
        }
        let (first_column, first_value) = &predicates[0];
        self.matching_rows(*first_column, first_value).into_iter().filter(|row_index| {
            predicates.iter().all(|(column_index, value)| self.rows[*row_index].values[*column_index] == *value)
        }).collect()
    }

    fn matching_rows_for_predicates_limited(
        &self,
        predicates: &[(usize, Value)],
        limit: Option<usize>,
    ) -> Vec<usize> {
        let take = limit.unwrap_or(usize::MAX);
        let columns = predicates.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        if let Some(index) = self.indexes.values().find(|index| index.column_indices == columns) {
            let values = predicates.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>();
            let key = IndexKey::from_columns(&values, &(0..values.len()).collect::<Vec<_>>());
            return index.entries.get(&key)
                .map(|row_ids| row_ids.iter().take(take)
                    .filter_map(|row_id| self.row_positions.get(row_id).copied())
                    .collect())
                .unwrap_or_default();
        }

        let (first_column, first_value) = &predicates[0];
        if let Some(index) = self.indexes.values().find(|index| {
            index.column_indices.len() == 1 && index.column_indices[0] == *first_column
        }) {
            let Some(candidates) = index.entries.get(&IndexKey::from(first_value)) else {
                return Vec::new();
            };
            return candidates.iter()
                .filter_map(|row_id| self.row_positions.get(row_id).copied())
                .filter(|row_index| predicates.iter().all(|(column_index, value)| {
                    self.rows[*row_index].values[*column_index] == *value
                }))
                .take(take)
                .collect();
        }

        self.rows.iter().enumerate()
            .filter(|(_, row)| predicates.iter().all(|(column_index, value)| row.values[*column_index] == *value))
            .take(take)
            .map(|(row_index, _)| row_index)
            .collect()
    }

    fn matching_rows(&self, column_index: usize, value: &Value) -> Vec<usize> {
        if let Some(index) = self.indexes.values().find(|index| index.column_indices.len() == 1 && index.column_indices[0] == column_index) {
            index.entries.get(&IndexKey::from(value))
                .map(|row_ids| self.positions_for_row_ids(row_ids))
                .unwrap_or_default()
        } else {
            self.rows.iter().enumerate()
                .filter_map(|(row_index, row)| (row.values[column_index] == *value).then_some(row_index))
                .collect()
        }
    }
}

#[derive(Debug, Clone)]
struct EmberIndexDelta {
    key: Vec<u8>,
    insert: bool,
}

fn build_row_index_deltas(
    table_name: &str,
    table: &Table,
    row: &Row,
    only_index: Option<&str>,
    insert: bool,
) -> Result<Option<Vec<EmberIndexDelta>>, DbError> {
    let mut deltas = Vec::new();
    for (index_name, index) in &table.indexes {
        if only_index.is_some_and(|selected| selected != index_name) {
            continue;
        }
        let key = IndexKey::from_columns(&row.values, &index.column_indices);
        let Some(key) = storage::encode_ember_index_entry(table_name, index_name, &key, row.row_id)? else {
            return Ok(None);
        };
        deltas.push(EmberIndexDelta { key, insert });
    }
    Ok(Some(deltas))
}

fn build_rows_index_deltas(
    table_name: &str,
    table: &Table,
    rows: &[Row],
    only_index: Option<&str>,
    insert: bool,
) -> Result<Option<Vec<EmberIndexDelta>>, DbError> {
    let mut all = Vec::new();
    for row in rows {
        let Some(mut row_deltas) = build_row_index_deltas(table_name, table, row, only_index, insert)? else {
            return Ok(None);
        };
        all.append(&mut row_deltas);
    }
    Ok(Some(all))
}

#[derive(Debug)]
pub struct Database {
    tables: BTreeMap<String, Table>,
    storage_path: Option<PathBuf>,
    wal_path: Option<PathBuf>,
    _lock_file: Option<Arc<std::fs::File>>,
    wal_sequence: u64,
    revision: u64,
    /// True when the PDB6 manifest is the durable SQL authority.
    pdb6_mode: bool,
    ember_indexes_valid: bool,
    ember_index_journal: Vec<EmberIndexDelta>,
    ember_journal_supported: bool,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            tables: BTreeMap::new(),
            storage_path: None,
            wal_path: None,
            _lock_file: None,
            wal_sequence: 0,
            revision: 0,
            pdb6_mode: false,
            ember_indexes_valid: false,
            ember_index_journal: Vec::new(),
            ember_journal_supported: true,
        }
    }
}

impl Database {
    pub fn new() -> Self { Self::default() }

    /// Opens the database's independent persistent key/value sidecar.
    pub fn open_key_value_store(&self) -> Result<KeyValueStore, DbError> {
        self.open_key_value_store_with_cache_capacity(128)
    }

    /// Opens the database's key/value sidecar with a configurable page-cache capacity.
    pub fn open_key_value_store_with_cache_capacity(&self, capacity_pages: usize) -> Result<KeyValueStore, DbError> {
        let path = self.storage_path.as_ref()
            .ok_or_else(|| DbError::Storage("in-memory databases do not have a persistent key/value sidecar".into()))?;
        let mut path = path.as_os_str().to_os_string();
        path.push(".kv");
        KeyValueStore::open_with_cache_capacity(PathBuf::from(path), capacity_pages)
    }

    /// Opens the database's independent variable-length record sidecar.
    ///
    /// Records use caller-supplied stable IDs and are not automatically enlisted
    /// in SQL transactions or maintained as a mirror of SQL table rows.
    pub fn open_record_store(&self) -> Result<RecordStore, DbError> {
        self.open_record_store_with_cache_capacity(128)
    }

    /// Opens the record sidecar with a configurable decoded-page cache capacity.
    pub fn open_record_store_with_cache_capacity(&self, capacity_pages: usize) -> Result<RecordStore, DbError> {
        let path = self.storage_path.as_ref()
            .ok_or_else(|| DbError::Storage("in-memory databases do not have a persistent record sidecar".into()))?;
        let mut path = path.as_os_str().to_os_string();
        path.push(".rows");
        RecordStore::open_with_cache_capacity(PathBuf::from(path), capacity_pages)
    }

    /// Opens the database's registry-like hierarchical configuration sidecar.
    pub fn open_registry(&self) -> Result<Registry, DbError> {
        let path = self.storage_path.as_ref()
            .ok_or_else(|| DbError::Storage("in-memory databases do not have a persistent registry sidecar".into()))?;
        let mut path = path.as_os_str().to_os_string();
        path.push(".registry");
        Registry::open(PathBuf::from(path))
    }

    fn append_ember_journal(&mut self, deltas: Option<Vec<EmberIndexDelta>>) {
        match deltas {
            Some(mut deltas) => self.ember_index_journal.append(&mut deltas),
            None => self.ember_journal_supported = false,
        }
    }

    /// Execute one SQL statement. Supported initially: CREATE TABLE, INSERT, SELECT *.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        let is_write = starts_with_kw(statement, "CREATE TABLE")
            || starts_with_kw(statement, "CREATE INDEX")
            || starts_with_kw(statement, "INSERT INTO")
            || starts_with_kw(statement, "UPDATE")
            || starts_with_kw(statement, "DELETE FROM");
        if is_write {
            return self.transaction(|transaction| transaction.execute(statement));
        }
        self.execute_inner(statement)
    }

    /// Execute SQL with typed positional parameters. Placeholders inside quoted
    /// string literals are left untouched; the parameter count must match exactly.
    pub fn execute_params(&mut self, sql: &str, parameters: &[Value]) -> Result<QueryResult, DbError> {
        let bound = bind_parameters(sql, parameters)?;
        self.execute(&bound)
    }

    fn execute_inner(&mut self, statement: &str) -> Result<QueryResult, DbError> {
        let result = if starts_with_kw(statement, "CREATE TABLE") {
            self.create_table(statement)
        } else if starts_with_kw(statement, "CREATE INDEX") {
            self.create_index(statement)
        } else if starts_with_kw(statement, "INSERT INTO") {
            self.insert(statement)
        } else if starts_with_kw(statement, "UPDATE") {
            self.update(statement)
        } else if starts_with_kw(statement, "DELETE FROM") {
            self.delete(statement)
        } else if starts_with_kw(statement, "SELECT") {
            self.select(statement)
        } else {
            Err(DbError::Unsupported(statement.to_owned()))
        };
        result
    }

    /// Start an owned optimistic session for a client connection.
    pub fn begin_session(&self) -> DatabaseSession {
        DatabaseSession {
            base_revision: self.revision,
            staged: Database {
                tables: self.tables.clone(),
                storage_path: None,
                wal_path: None,
                _lock_file: None,
                wal_sequence: self.wal_sequence,
                revision: self.revision,
                pdb6_mode: false,
                ember_indexes_valid: false,
                ember_index_journal: Vec::new(),
                ember_journal_supported: true,
            },
            statements: Vec::new(),
        }
    }

    /// Commit a previously staged session if no other writer changed the database.
    pub fn commit_session(&mut self, session: DatabaseSession) -> Result<(), DbError> {
        if session.base_revision != self.revision {
            return Err(DbError::TransactionConflict);
        }
        if session.statements.is_empty() { return Ok(()); }
        if self.pdb6_mode {
            let path = self.storage_path.as_deref()
                .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
            let sequence = self.wal_sequence.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
            crate::storage::persist_pdb6_transition(path, &self.tables, &session.staged.tables, sequence)?;
            self.wal_sequence = sequence;
        } else if self.wal_path.is_some() {
            let sequence = self.append_wal_transaction(&session.statements)?;
            self.wal_sequence = sequence;
        }
        let previous_mirror_valid = self.ember_indexes_valid;
        self.tables = session.staged.tables;
        self.revision = self.revision.wrapping_add(1);
        self.ember_indexes_valid = false;
        // PDB6 owns the catalog, rows and indexes in one root publication. The
        // legacy EmberTree index mirror is used only by PDB5 databases.
        if !self.pdb6_mode {
            self.ember_indexes_valid = self
                .sync_ember_indexes_journal(
                    &session.staged.ember_index_journal,
                    session.staged.ember_journal_supported,
                    previous_mirror_valid,
                )
                .unwrap_or(false);
        }
        Ok(())
    }

    /// Run multiple SQL statements as one atomic transaction.
    /// The transaction is durable only after its WAL commit record is synced.
    pub fn transaction<F, T>(&mut self, operation: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T, DbError>,
    {
        let staged = Database {
            tables: self.tables.clone(),
            storage_path: None,
            wal_path: None,
            _lock_file: None,
            wal_sequence: self.wal_sequence,
            revision: self.revision,
            pdb6_mode: false,
            ember_indexes_valid: false,
            ember_index_journal: Vec::new(),
            ember_journal_supported: true,
        };
        let mut transaction = Transaction {
            database: self,
            staged,
            statements: Vec::new(),
        };
        let value = operation(&mut transaction)?;
        transaction.commit()?;
        Ok(value)
    }

    /// Insert a bounded batch of rows in one operation.
    ///
    /// Every row is validated before the table is changed. Persistent databases
    /// are written once per batch instead of once per row. The limit matches
    /// KSL's default maximum batch size; callers should split larger imports.
    pub fn insert_many<I>(&mut self, table_name: &str, rows: I) -> Result<usize, DbError>
    where
        I: IntoIterator<Item = Vec<Value>>,
    {
        const MAX_BATCH_ROWS: usize = 5_000;
        let batch = rows.into_iter().take(MAX_BATCH_ROWS + 1).collect::<Vec<_>>();
        if batch.len() > MAX_BATCH_ROWS {
            return Err(DbError::Constraint(format!(
                "batch exceeds the maximum of {MAX_BATCH_ROWS} rows"
            )));
        }
        if batch.is_empty() { return Ok(0); }

        let normalized = normalize_ident(table_name);
        let table = self.tables.get(&normalized)
            .ok_or_else(|| DbError::TableNotFound(normalized.clone()))?;

        // Validate the complete batch before mutating the table: no partial inserts.
        for (row_index, values) in batch.iter().enumerate() {
            if values.len() != table.columns.len() {
                return Err(DbError::Constraint(format!(
                    "row {}: expected {} values, received {}",
                    row_index + 1, table.columns.len(), values.len()
                )));
            }
            for (column, value) in table.columns.iter().zip(values.iter()) {
                validate_value(column, value)?;
            }
        }

        let inserted = batch.len();
        self.transaction(|transaction| {
            for values in batch {
                let encoded = values.iter().map(value_to_sql).collect::<Vec<_>>().join(", ");
                transaction.execute(&format!("INSERT INTO {normalized} VALUES ({encoded})"))?;
            }
            Ok(inserted)
        })
    }

    fn create_table(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let mut rest = sql["CREATE TABLE".len()..].trim();
        let if_not_exists = starts_with_kw(rest, "IF NOT EXISTS");
        if if_not_exists { rest = rest["IF NOT EXISTS".len()..].trim(); }
        let open = rest.find('(').ok_or_else(|| DbError::Parse("expected '(' after table name".into()))?;
        let close = rest.rfind(')').ok_or_else(|| DbError::Parse("expected ')'".into()))?;
        let name = normalize_ident(rest[..open].trim());
        if name.is_empty() { return Err(DbError::Parse("missing table name".into())); }
        if self.tables.contains_key(&name) {
            return if if_not_exists {
                Ok(QueryResult::message(format!("table {name} already exists")))
            } else {
                Err(DbError::TableExists(name))
            };
        }

        let mut columns = Vec::new();
        let mut primary_key_columns = Vec::new();
        for definition in split_csv(&rest[open + 1..close]) {
            let upper = definition.to_ascii_uppercase();
            if upper.starts_with("PRIMARY KEY") {
                let key_open = definition.find('(').ok_or_else(|| DbError::Parse("expected '(' in PRIMARY KEY".into()))?;
                let key_close = definition.rfind(')').ok_or_else(|| DbError::Parse("expected ')' in PRIMARY KEY".into()))?;
                primary_key_columns.extend(split_csv(&definition[key_open + 1..key_close]).into_iter().map(normalize_ident));
                continue;
            }

            let mut parts = definition.split_whitespace();
            let column_name = parts.next().ok_or_else(|| DbError::Parse("empty column definition".into()))?;
            let type_token = parts.next().ok_or_else(|| DbError::Parse(format!("missing type for {column_name}")))?;
            let type_name = type_token.split('(').next().unwrap_or(type_token).to_ascii_uppercase();
            let data_type = match type_name.as_str() {
                "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => DataType::Integer,
                "TEXT" | "LONGTEXT" | "MEDIUMTEXT" | "VARCHAR" | "CHAR" | "STRING" => DataType::Text,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                other => return Err(DbError::Unsupported(format!("column type {other}"))),
            };
            let primary = upper.contains("PRIMARY KEY");
            let nullable = !upper.contains("NOT NULL") && !primary;
            let normalized_column = normalize_ident(column_name);
            if primary { primary_key_columns.push(normalized_column.clone()); }
            columns.push(Column { name: normalized_column, data_type, nullable });
        }
        if columns.is_empty() { return Err(DbError::Parse("a table needs at least one column".into())); }
        for column in &mut columns {
            if primary_key_columns.contains(&column.name) { column.nullable = false; }
        }
        let mut table = Table {
            columns,
            rows: Vec::new(),
            next_row_id: 1,
            row_positions: BTreeMap::new(),
            indexes: BTreeMap::new(),
        };
        if !primary_key_columns.is_empty() {
            table.add_index_columns(format!("__pk_{name}"), &primary_key_columns, true)?;
        }
        self.tables.insert(name.clone(), table);
        Ok(QueryResult::message(format!("table {name} created")))
    }

    fn create_index(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["CREATE INDEX".len()..].trim();
        let on_pos = rest.to_ascii_uppercase().find(" ON ")
            .ok_or_else(|| DbError::Parse("expected ON in CREATE INDEX".into()))?;
        let index_name = normalize_ident(rest[..on_pos].trim());
        let target = rest[on_pos + 4..].trim();
        let open = target.find('(').ok_or_else(|| DbError::Parse("expected '(' with indexed column".into()))?;
        let close = target.rfind(')').ok_or_else(|| DbError::Parse("expected ')' after indexed column".into()))?;
        let table_name = normalize_ident(target[..open].trim());
        let column_name = normalize_ident(target[open + 1..close].trim());
        if index_name.is_empty() || column_name.is_empty() {
            return Err(DbError::Parse("index and column names are required".into()));
        }
        let deltas = {
            let table = self.tables.get_mut(&table_name)
                .ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
            table.add_index(index_name.clone(), &column_name)?;
            build_rows_index_deltas(&table_name, table, &table.rows, Some(&index_name), true)?
        };
        self.append_ember_journal(deltas);
        Ok(QueryResult::message(format!("index {index_name} created on {table_name}({column_name})")))
    }

    fn insert(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["INSERT INTO".len()..].trim();
        let values_pos = find_kw(rest, "VALUES").ok_or_else(|| DbError::Parse("expected VALUES".into()))?;
        let target = rest[..values_pos].trim();
        let (table_name, column_names) = if let Some(open) = target.find('(') {
            let (columns_text, _) = take_parenthesized(&target[open..])?;
            let names = split_csv(columns_text).into_iter().map(normalize_ident).collect::<Vec<_>>();
            (normalize_ident(target[..open].trim()), Some(names))
        } else {
            (normalize_ident(target), None)
        };

        let values_text = rest[values_pos + "VALUES".len()..].trim();
        let (values_part, upsert_clause) = take_parenthesized(values_text)?;
        let supplied = split_csv(values_part).into_iter().map(parse_value).collect::<Result<Vec<_>, _>>()?;
        let table = self.tables.get_mut(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        let values = if let Some(names) = column_names {
            if names.len() != supplied.len() {
                return Err(DbError::Constraint(format!("{} insert columns but {} values", names.len(), supplied.len())));
            }
            let mut row = vec![Value::Null; table.columns.len()];
            let mut assigned = std::collections::BTreeSet::new();
            for (name, value) in names.iter().zip(supplied.into_iter()) {
                let index = table.columns.iter().position(|column| column.name == *name)
                    .ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
                if !assigned.insert(index) {
                    return Err(DbError::Constraint(format!("column listed more than once: {name}")));
                }
                row[index] = value;
            }
            row
        } else {
            if supplied.len() != table.columns.len() {
                return Err(DbError::Constraint(format!("expected {} values, received {}", table.columns.len(), supplied.len())));
            }
            supplied
        };
        for (column, value) in table.columns.iter().zip(values.iter()) {
            validate_value(column, value)?;
        }

        if !upsert_clause.trim().is_empty() {
            let existing_row_index = table.indexes.values().filter(|index| index.unique).find_map(|index| {
                let key = IndexKey::from_columns(&values, &index.column_indices);
                index.entries.get(&key).and_then(|row_ids| row_ids.first()).and_then(|row_id| table.row_positions.get(row_id)).copied()
            });
            let old_row = existing_row_index.and_then(|position| table.rows.get(position).cloned());
            if apply_upsert(table, &values, upsert_clause)? {
                let new_row = existing_row_index.and_then(|position| table.rows.get(position).cloned());
                let mut deltas = Vec::new();
                if let Some(row) = old_row {
                    let Some(mut old) = build_row_index_deltas(&table_name, table, &row, None, false)? else {
                        self.ember_journal_supported = false;
                        return Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row updated by upsert".into() });
                    };
                    deltas.append(&mut old);
                }
                if let Some(row) = new_row {
                    let Some(mut new) = build_row_index_deltas(&table_name, table, &row, None, true)? else {
                        self.ember_journal_supported = false;
                        return Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row updated by upsert".into() });
                    };
                    deltas.append(&mut new);
                }
                self.ember_index_journal.append(&mut deltas);
                return Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row updated by upsert".into() });
            }
        }
        table.validate_unique(&values)?;
        let row_id = table.next_row_id;
        table.next_row_id = table.next_row_id.checked_add(1)
            .ok_or_else(|| DbError::Storage("table row identifier space exhausted".into()))?;
        table.rows.push(Row { row_id, values });
        let row_index = table.rows.len() - 1;
        table.row_positions.insert(row_id, row_index);
        table.index_row(row_index);
        let inserted_row = table.rows[row_index].clone();
        let deltas = build_row_index_deltas(&table_name, table, &inserted_row, None, true)?;
        self.append_ember_journal(deltas);
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row inserted".into() })
    }

    fn update(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["UPDATE".len()..].trim();
        let set_pos = find_kw(rest, "SET").ok_or_else(|| DbError::Parse("expected SET in UPDATE".into()))?;
        let table_name = normalize_ident(rest[..set_pos].trim());
        let after_set = rest[set_pos + 3..].trim();
        let where_pos = after_set.to_ascii_uppercase().find(" WHERE ");
        let (assignment, predicate) = match where_pos {
            Some(position) => (&after_set[..position], Some(after_set[position + 7..].trim())),
            None => (after_set, None),
        };
        let equals = assignment.find('=').ok_or_else(|| DbError::Parse("expected '=' in UPDATE assignment".into()))?;
        let target_column = normalize_ident(assignment[..equals].trim());
        let new_value = parse_value(assignment[equals + 1..].trim())?;

        let (affected, deltas) = {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
            let target_index = table.columns.iter().position(|column| column.name == target_column)
                .ok_or_else(|| DbError::ColumnNotFound(target_column.clone()))?;
            validate_value(&table.columns[target_index], &new_value)?;

            let matching = if let Some(predicate) = predicate {
                let predicates = parse_equality_predicates(&table.columns, predicate)?;
                table.matching_rows_for_predicates(&predicates)
            } else {
                (0..table.rows.len()).collect()
            };
            table.validate_unique_update(&matching, target_index, &new_value)?;
            let old_rows = matching.iter().filter_map(|position| table.rows.get(*position).cloned()).collect::<Vec<_>>();
            for row_index in &matching {
                let old_values = table.rows[*row_index].values.clone();
                if old_values[target_index] != new_value {
                    table.rows[*row_index].values[target_index] = new_value.clone();
                    table.reindex_row(*row_index, &old_values);
                }
            }
            let new_rows = old_rows.iter().filter_map(|old| {
                table.row_positions.get(&old.row_id).and_then(|position| table.rows.get(*position)).cloned()
            }).collect::<Vec<_>>();
            let old_deltas = build_rows_index_deltas(&table_name, table, &old_rows, None, false)?;
            let new_deltas = build_rows_index_deltas(&table_name, table, &new_rows, None, true)?;
            match (old_deltas, new_deltas) {
                (Some(mut old), Some(mut new)) => { old.append(&mut new); (matching.len(), Some(old)) }
                _ => (matching.len(), None),
            }
        };
        self.append_ember_journal(deltas);
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: affected, message: format!("{affected} row(s) updated") })
    }

    fn delete(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["DELETE FROM".len()..].trim();
        let where_pos = rest.to_ascii_uppercase().find(" WHERE ");
        let (table_part, predicate) = match where_pos {
            Some(position) => (&rest[..position], Some(rest[position + 7..].trim())),
            None => (rest, None),
        };
        let table_name = normalize_ident(table_part);
        let (removed, deltas) = {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
            let matches = if let Some(predicate) = predicate {
                let predicates = parse_equality_predicates(&table.columns, predicate)?;
                table.matching_rows_for_predicates(&predicates).into_iter().collect::<std::collections::BTreeSet<_>>()
            } else {
                (0..table.rows.len()).collect::<std::collections::BTreeSet<_>>()
            };
            let old_rows = matches.iter().filter_map(|position| table.rows.get(*position).cloned()).collect::<Vec<_>>();
            let before = table.rows.len();
            for row in &old_rows {
                table.remove_index_row(row);
            }
            let retained = table.rows.drain(..).enumerate()
                .filter_map(|(index, row)| (!matches.contains(&index)).then_some(row))
                .collect::<Vec<_>>();
            table.rows = retained;
            table.rebuild_row_positions();
            let removed = before - table.rows.len();
            let deltas = build_rows_index_deltas(&table_name, table, &old_rows, None, false)?;
            (removed, deltas)
        };
        self.append_ember_journal(deltas);
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: removed, message: format!("{removed} row(s) deleted") })
    }

    fn select(&self, sql: &str) -> Result<QueryResult, DbError> {
        let upper = sql.to_ascii_uppercase();
        let from = upper.find(" FROM ").ok_or_else(|| DbError::Parse("expected FROM".into()))?;
        let projection = sql["SELECT".len()..from].trim();
        let tail = sql[from + 6..].trim();
        let upper_tail = tail.to_ascii_uppercase();

        let (query_tail, limit) = if let Some(position) = upper_tail.rfind(" LIMIT ") {
            let limit_text = tail[position + 7..].trim();
            if limit_text.split_whitespace().count() != 1 {
                return Err(DbError::Parse("LIMIT expects one non-negative integer".into()));
            }
            let limit = limit_text.parse::<usize>()
                .map_err(|_| DbError::Parse("LIMIT expects one non-negative integer".into()))?;
            (&tail[..position], Some(limit))
        } else {
            (tail, None)
        };

        let query_upper = query_tail.to_ascii_uppercase();
        let where_pos = query_upper.find(" WHERE ");
        let (table_part, predicate) = match where_pos {
            Some(position) => (&query_tail[..position], Some(query_tail[position + 7..].trim())),
            None => (query_tail, None),
        };
        let table_name = normalize_ident(table_part.split_whitespace().next().unwrap_or(""));
        let table = self.tables.get(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;

        let projection_upper = projection.to_ascii_uppercase();
        let is_count = projection_upper.starts_with("COUNT(*)");
        let selection_limit = if is_count { None } else { limit };
        let selected_rows: Vec<usize> = if let Some(predicate) = predicate {
            let predicates = parse_equality_predicates(&table.columns, predicate)?;
            let columns = predicates.iter().map(|(index, _)| *index).collect::<Vec<_>>();
            let persistent_index = if self.ember_indexes_valid {
                self.storage_path.as_deref().and_then(|path| {
                    table.indexes.iter().find(|(_, index)| index.column_indices == columns)
                        .map(|(name, _)| (path, name.as_str()))
                })
            } else {
                None
            };
            if let Some((path, index_name)) = persistent_index {
                let values = predicates.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>();
                let key = IndexKey::from_columns(&values, &(0..values.len()).collect::<Vec<_>>());
                if let Some(row_ids) = storage::lookup_ember_index(path, &table_name, index_name, &key, selection_limit)? {
                    if row_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
                        return Err(DbError::Storage("persistent SQL index contains non-increasing stable row identifiers".into()));
                    }
                    let mut positions = Vec::with_capacity(row_ids.len());
                    for row_id in row_ids.into_iter().take(selection_limit.unwrap_or(usize::MAX)) {
                        let position = table.row_positions.get(&row_id).copied()
                            .ok_or_else(|| DbError::Storage("persistent SQL index references an unknown stable row identifier".into()))?;
                        positions.push(position);
                    }
                    positions
                } else {
                    table.matching_rows_for_predicates_limited(&predicates, selection_limit)
                }
            } else {
                table.matching_rows_for_predicates_limited(&predicates, selection_limit)
            }
        } else {
            (0..table.rows.len().min(selection_limit.unwrap_or(usize::MAX))).collect()
        };

        if is_count {
            let alias = projection_upper.find(" AS ").map(|position| normalize_ident(&projection[position + 4..]))
                .unwrap_or_else(|| "count".into());
            return Ok(QueryResult {
                columns: vec![alias],
                rows: vec![Row { row_id: 0, values: vec![Value::Integer(selected_rows.len() as i64)] }],
                message: "1 row(s)".into(),
                affected_rows: 0,
            });
        }

        let projection_indices = if projection == "*" {
            (0..table.columns.len()).collect::<Vec<_>>()
        } else {
            split_csv(projection).into_iter().map(|name| {
                let normalized = normalize_ident(name);
                table.columns.iter().position(|column| column.name == normalized)
                    .ok_or_else(|| DbError::ColumnNotFound(normalized))
            }).collect::<Result<Vec<_>, _>>()?
        };
        let columns = projection_indices.iter().map(|index| table.columns[*index].name.clone()).collect();
        let rows = selected_rows.into_iter().filter_map(|row_index| {
            table.rows.get(row_index).map(|row| Row {
                row_id: row.row_id,
                values: projection_indices.iter().map(|column_index| row.values[*column_index].clone()).collect()
            })
        }).collect::<Vec<_>>();
        Ok(QueryResult {
            message: format!("{} row(s)", rows.len()),
            rows,
            columns,
            affected_rows: 0,
        })
    }
}


/// An owned transaction session suitable for request/response server protocols.
/// Changes remain private until Database::commit_session accepts the base revision.
pub struct DatabaseSession {
    base_revision: u64,
    staged: Database,
    statements: Vec<String>,
}

impl DatabaseSession {
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        let is_write = starts_with_kw(statement, "CREATE TABLE")
            || starts_with_kw(statement, "CREATE INDEX")
            || starts_with_kw(statement, "INSERT INTO")
            || starts_with_kw(statement, "UPDATE")
            || starts_with_kw(statement, "DELETE FROM");
        let result = self.staged.execute_inner(statement)?;
        if is_write { self.statements.push(statement.to_owned()); }
        Ok(result)
    }

    pub fn execute_params(&mut self, sql: &str, parameters: &[Value]) -> Result<QueryResult, DbError> {
        let bound = bind_parameters(sql, parameters)?;
        self.execute(&bound)
    }

    pub fn has_writes(&self) -> bool { !self.statements.is_empty() }
}

/// A transaction stages changes privately until its commit record is durable.
pub struct Transaction<'a> {
    database: &'a mut Database,
    staged: Database,
    statements: Vec<String>,
}

impl Transaction<'_> {
    /// Execute SQL against the transaction's private view.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        let is_write = starts_with_kw(statement, "CREATE TABLE")
            || starts_with_kw(statement, "CREATE INDEX")
            || starts_with_kw(statement, "INSERT INTO")
            || starts_with_kw(statement, "UPDATE")
            || starts_with_kw(statement, "DELETE FROM");
        let result = self.staged.execute_inner(statement)?;
        if is_write {
            self.statements.push(statement.to_owned());
        }
        Ok(result)
    }

    /// Execute a parameterized statement inside this transaction.
    pub fn execute_params(&mut self, sql: &str, parameters: &[Value]) -> Result<QueryResult, DbError> {
        let bound = bind_parameters(sql, parameters)?;
        self.execute(&bound)
    }

    /// Commit all staged writes as one WAL transaction.
    pub fn commit(self) -> Result<(), DbError> {
        if self.statements.is_empty() {
            return Ok(());
        }
        if self.database.pdb6_mode {
            let path = self.database.storage_path.as_deref()
                .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
            let sequence = self.database.wal_sequence.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
            crate::storage::persist_pdb6_transition(
                path, &self.database.tables, &self.staged.tables, sequence,
            )?;
            self.database.wal_sequence = sequence;
        } else if self.database.wal_path.is_some() {
            let sequence = self.database.append_wal_transaction(&self.statements)?;
            self.database.wal_sequence = sequence;
        }
        let previous_mirror_valid = self.database.ember_indexes_valid;
        self.database.tables = self.staged.tables;
        self.database.revision = self.database.revision.wrapping_add(1);
        self.database.ember_indexes_valid = false;
        // PDB6 publishes SQL catalog, rows and indexes through one root. PDB5
        // continues to use its SQL WAL and derived EmberTree index mirror.
        if !self.database.pdb6_mode {
            self.database.ember_indexes_valid = self.database
                .sync_ember_indexes_journal(
                    &self.staged.ember_index_journal,
                    self.staged.ember_journal_supported,
                    previous_mirror_valid,
                )
                .unwrap_or(false);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pdb6_manifest_becomes_live_sql_authority_after_reopen() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-pdb6-live-{}-{}.pbd",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE players (uuid TEXT NOT NULL, coins INTEGER)").unwrap();
            db.execute("CREATE INDEX players_uuid ON players (uuid)").unwrap();
            db.execute("INSERT INTO players VALUES ('player-a', 10)").unwrap();
            db.execute("INSERT INTO players VALUES ('player-b', 20)").unwrap();
            db.migrate_to_pdb6().unwrap();
        }
        {
            let mut db = Database::open(&path).unwrap();
            assert_eq!(db.execute("SELECT * FROM players WHERE uuid = 'player-a'").unwrap().rows.len(), 1);
            db.execute("UPDATE players SET coins = 99 WHERE uuid = 'player-a'").unwrap();
            db.execute("DELETE FROM players WHERE uuid = 'player-b'").unwrap();
            db.execute("INSERT INTO players VALUES ('player-c', 30)").unwrap();
        }
        {
            let mut db = Database::open(&path).unwrap();
            let rows = db.execute("SELECT * FROM players").unwrap().rows;
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().any(|row| row.values == vec![Value::Text("player-a".into()), Value::Integer(99)]));
            assert!(rows.iter().any(|row| row.values == vec![Value::Text("player-c".into()), Value::Integer(30)]));
            assert!(db.execute("SELECT * FROM players WHERE uuid = 'player-b'").unwrap().rows.is_empty());
        }
        for suffix in [
            "", ".wal", ".lock", ".pdb6.lock", ".pdb6.pages", ".pdb6.wal",
            ".pages", ".pages.wal", ".pages.lock",
        ] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }

    #[test]
    fn database_opens_independent_key_value_and_registry_sidecars() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-kv-registry-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let db = Database::open(&path).unwrap();
            let mut kv = db.open_key_value_store().unwrap();
            kv.put(b"world/seed", b"12345").unwrap();
            drop(kv);
            let mut registry = db.open_registry().unwrap();
            registry.create_hive("SYSTEM").unwrap();
            registry.create_key("SYSTEM/Storage").unwrap();
            registry.set_value("SYSTEM/Storage", "Mode", RegistryValue::String("EmberTree".into())).unwrap();
        }
        {
            let db = Database::open(&path).unwrap();
            let mut kv = db.open_key_value_store().unwrap();
            assert_eq!(kv.get(b"world/seed").unwrap(), Some(b"12345".to_vec()));
            drop(kv);
            let mut registry = db.open_registry().unwrap();
            assert_eq!(registry.get_value("system/storage", "mode").unwrap(),
                Some(RegistryValue::String("EmberTree".into())));
        }
        for suffix in [
            ".pdb", ".pdb.wal", ".pdb.lock",
            ".pdb.kv.lock", ".pdb.kv.pages", ".pdb.kv.pages.wal",
            ".pdb.registry.lock", ".pdb.registry.pages", ".pdb.registry.pages.wal",
        ] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix.strip_prefix(".pdb").unwrap_or(suffix)));
        }
    }

    #[test]
    fn sql_transactions_journal_only_affected_index_rows() {
        let mut db = Database::new();
        db.execute("CREATE TABLE journal_players (uuid TEXT, coins INTEGER)").unwrap();
        db.execute("INSERT INTO journal_players VALUES ('player-a', 10)").unwrap();
        db.execute("INSERT INTO journal_players VALUES ('player-b', 20)").unwrap();
        db.execute("CREATE INDEX journal_players_uuid ON journal_players(uuid)").unwrap();

        db.transaction(|tx| {
            tx.execute("INSERT INTO journal_players VALUES ('player-c', 30)")?;
            assert_eq!(tx.staged.ember_index_journal.len(), 1, "one inserted row should journal one index key");
            tx.execute("UPDATE journal_players SET uuid = 'player-a2' WHERE uuid = 'player-a'")?;
            assert_eq!(tx.staged.ember_index_journal.len(), 3, "an indexed-key update should journal one removal and one insertion");
            tx.execute("DELETE FROM journal_players WHERE uuid = 'player-b'")?;
            assert_eq!(tx.staged.ember_index_journal.len(), 4, "one deleted row should journal one removal");
            Ok(())
        }).unwrap();

        assert_eq!(db.execute("SELECT * FROM journal_players WHERE uuid = 'player-a2'").unwrap().rows.len(), 1);
        assert_eq!(db.execute("SELECT * FROM journal_players WHERE uuid = 'player-b'").unwrap().rows.len(), 0);
        assert_eq!(db.execute("SELECT * FROM journal_players WHERE uuid = 'player-c'").unwrap().rows.len(), 1);
    }

    #[test]
    fn creates_inserts_and_selects() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Ada')").unwrap();
        let result = db.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, vec![Value::Integer(1), Value::Text("Ada".into())]);
    }


    #[test]
    fn bulk_insert_validates_then_persists_once() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-batch-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE player_stats (uuid TEXT NOT NULL, coins INTEGER)").unwrap();
            let inserted = db.insert_many("player_stats", vec![
                vec![Value::Text("player-a".into()), Value::Integer(100)],
                vec![Value::Text("player-b".into()), Value::Integer(250)],
            ]).unwrap();
            assert_eq!(inserted, 2);

            let error = db.insert_many("player_stats", vec![
                vec![Value::Text("player-c".into()), Value::Integer(300)],
                vec![Value::Text("player-d".into()), Value::Text("invalid".into())],
            ]);
            assert!(error.is_err());
            assert_eq!(db.execute("SELECT * FROM player_stats").unwrap().rows.len(), 2);
        }
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.execute("SELECT * FROM player_stats").unwrap().rows.len(), 2);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bulk_insert_rejects_oversized_batches_without_partial_write() {
        let mut db = Database::new();
        db.execute("CREATE TABLE counters (value INTEGER)").unwrap();
        let rows = (0..5_001).map(|n| vec![Value::Integer(n)]);
        assert!(db.insert_many("counters", rows).is_err());
        assert_eq!(db.execute("SELECT * FROM counters").unwrap().rows.len(), 0);
    }


    #[test]
    fn persistent_wal_replays_committed_sql_transaction() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        {
            let mut db = Database::open(&path).unwrap();
            db.transaction(|tx| {
                tx.execute("CREATE TABLE players (uuid TEXT NOT NULL, coins INTEGER)")?;
                tx.execute("INSERT INTO players VALUES ('abc-123', 900)")?;
                tx.execute("INSERT INTO players VALUES ('def-456', 1200)")?;
                Ok(())
            }).unwrap();
            assert!(wal_path.exists());
        }
        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM players").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[0], Value::Text("abc-123".into()));
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&wal_path).unwrap();
    }

    #[test]
    fn failed_transaction_leaves_no_partial_changes() {
        let mut db = Database::new();
        db.execute("CREATE TABLE values_table (value INTEGER)").unwrap();
        let result = db.transaction(|tx| {
            tx.execute("INSERT INTO values_table VALUES (1)")?;
            tx.execute("INSERT INTO values_table VALUES ('bad')")?;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(db.execute("SELECT * FROM values_table").unwrap().rows.len(), 0);
    }

    #[test]
    fn persistent_bulk_insert_uses_wal_and_survives_reopen() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-wal-bulk-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let wal_path = std::path::PathBuf::from(format!("{}.wal", path.display()));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE stats (uuid TEXT, coins INTEGER)").unwrap();
            assert_eq!(db.insert_many("stats", vec![
                vec![Value::Text("a'bc".into()), Value::Integer(10)],
                vec![Value::Text("def".into()), Value::Integer(20)],
            ]).unwrap(), 2);
        }
        let mut reopened = Database::open(&path).unwrap();
        assert_eq!(reopened.execute("SELECT * FROM stats").unwrap().rows.len(), 2);
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(&wal_path).unwrap();
    }


    #[test]
    fn indexed_equality_query_returns_matching_rows() {
        let mut db = Database::new();
        db.execute("CREATE TABLE players (uuid TEXT NOT NULL, coins INTEGER)").unwrap();
        db.execute("INSERT INTO players VALUES ('uuid-a', 10)").unwrap();
        db.execute("INSERT INTO players VALUES ('uuid-b', 20)").unwrap();
        db.execute("CREATE INDEX idx_players_uuid ON players (uuid)").unwrap();
        db.execute("INSERT INTO players VALUES ('uuid-a', 30)").unwrap();
        let result = db.execute("SELECT * FROM players WHERE uuid = 'uuid-a'").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].values[0], Value::Text("uuid-a".into()));
        assert_eq!(result.rows[1].values[1], Value::Integer(30));
    }

    #[test]
    fn indexed_where_returns_empty_for_missing_key() {
        let mut db = Database::new();
        db.execute("CREATE TABLE players (uuid TEXT)").unwrap();
        db.execute("CREATE INDEX idx_uuid ON players (uuid)").unwrap();
        assert!(db.execute("SELECT * FROM players WHERE uuid = 'missing'").unwrap().rows.is_empty());
    }


    #[test]
    fn persistent_index_falls_back_for_keys_larger_than_ember_tree_limit() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-long-index-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let value = "x".repeat(2048);
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE long_keys (key TEXT)").unwrap();
            db.execute("CREATE INDEX idx_long_key ON long_keys (key)").unwrap();
            db.execute_params("INSERT INTO long_keys VALUES (?)", &[Value::Text(value.clone())]).unwrap();
            assert!(!db.ember_indexes_valid);
            let result = db.execute_params(
                "SELECT * FROM long_keys WHERE key = ?",
                &[Value::Text(value.clone())],
            ).unwrap();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values[0], Value::Text(value));
        }
        for suffix in [".pdb", ".pdb.wal", ".pdb.lock", ".pdb.pages", ".pdb.pages.btree.wal"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix.strip_prefix(".pdb").unwrap_or(suffix)));
        }
    }

    #[test]
    fn stable_row_ids_are_not_reused_after_delete_all_and_checkpoint() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-stable-row-id-empty-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE row_id_reuse (value TEXT)").unwrap();
            db.execute("INSERT INTO row_id_reuse VALUES ('first')").unwrap();
            db.execute("INSERT INTO row_id_reuse VALUES ('second')").unwrap();
            db.execute("DELETE FROM row_id_reuse").unwrap();
            db.checkpoint().unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        reopened.execute("INSERT INTO row_id_reuse VALUES ('third')").unwrap();
        let row = reopened.execute("SELECT * FROM row_id_reuse WHERE value = 'third'").unwrap();
        assert_eq!(row.rows[0].row_id, 3, "checkpoint must preserve the next ID even when no rows remain");
        drop(reopened);
        for suffix in [".pdb", ".pdb.wal", ".pdb.lock", ".pdb.pages", ".pdb.pages.btree.wal"] {
            let _ = std::fs::remove_file(format!("{}{}", path.display(), suffix.strip_prefix(".pdb").unwrap_or(suffix)));
        }
    }

    #[test]
    fn persistent_ember_index_applies_incremental_sql_insert_update_delete_deltas() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-index-delta-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let player_c_id;
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE delta_players (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("INSERT INTO delta_players VALUES ('player-a', 10)").unwrap();
            db.execute("INSERT INTO delta_players VALUES ('player-b', 20)").unwrap();
            db.execute("INSERT INTO delta_players VALUES ('player-c', 30)").unwrap();
            db.execute("CREATE INDEX idx_delta_uuid ON delta_players (uuid)").unwrap();
            assert!(db.ember_indexes_valid);

            db.execute("INSERT INTO delta_players VALUES ('player-d', 40)").unwrap();
            assert!(db.ember_indexes_valid);
            player_c_id = db.execute("SELECT * FROM delta_players WHERE uuid = 'player-c'")
                .unwrap().rows[0].row_id;
            db.execute("UPDATE delta_players SET uuid = 'player-a2' WHERE uuid = 'player-a'").unwrap();
            assert!(db.ember_indexes_valid);
            db.execute("DELETE FROM delta_players WHERE uuid = 'player-b'").unwrap();
            assert!(db.ember_indexes_valid);

            assert_eq!(db.execute("SELECT * FROM delta_players WHERE uuid = 'player-a'").unwrap().rows.len(), 0);
            assert_eq!(db.execute("SELECT * FROM delta_players WHERE uuid = 'player-a2'").unwrap().rows.len(), 1);
            assert_eq!(db.execute("SELECT * FROM delta_players WHERE uuid = 'player-b'").unwrap().rows.len(), 0);
            let player_c_after_delete = db.execute("SELECT * FROM delta_players WHERE uuid = 'player-c'")
                .unwrap().rows[0].row_id;
            assert_eq!(player_c_after_delete, player_c_id, "deleting an earlier row must not renumber later rows");
            assert_eq!(db.execute("SELECT * FROM delta_players WHERE uuid = 'player-d'").unwrap().rows.len(), 1);
            db.execute("INSERT INTO delta_players VALUES ('player-e', 50)").unwrap();
            let player_e_id = db.execute("SELECT * FROM delta_players WHERE uuid = 'player-e'")
                .unwrap().rows[0].row_id;
            assert_eq!(player_e_id, 5, "deleted row IDs must not be reused");
            db.transaction(|tx| {
                tx.execute("INSERT INTO delta_players VALUES ('ephemeral', 60)")?;
                tx.execute("DELETE FROM delta_players WHERE uuid = 'ephemeral'")?;
                Ok(())
            }).unwrap();
            assert!(db.ember_indexes_valid);
            assert_eq!(db.execute("SELECT * FROM delta_players WHERE uuid = 'ephemeral'").unwrap().rows.len(), 0);
            db.checkpoint().unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        assert!(reopened.ember_indexes_valid);
        assert_eq!(reopened.execute("SELECT * FROM delta_players WHERE uuid = 'player-a2'").unwrap().rows.len(), 1);
        assert_eq!(reopened.execute("SELECT * FROM delta_players WHERE uuid = 'player-b'").unwrap().rows.len(), 0);
        assert_eq!(reopened.execute("SELECT * FROM delta_players WHERE uuid = 'player-e'").unwrap().rows[0].row_id, 5);
        let player_c_reopened = reopened.execute("SELECT * FROM delta_players WHERE uuid = 'player-c'")
            .unwrap().rows[0].row_id;
        assert_eq!(player_c_reopened, player_c_id, "PDB5 checkpoint must preserve stable row identifiers");
        drop(reopened);
        for suffix in [".pdb", ".pdb.wal", ".pdb.lock", ".pdb.pages", ".pdb.pages.btree.wal"] {
            let candidate = format!("{}{}", path.display(), suffix.strip_prefix(".pdb").unwrap_or(suffix));
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn persistent_index_is_rebuilt_after_checkpoint_and_reopen() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-index-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE player_stats (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("INSERT INTO player_stats VALUES ('player-x', 50)").unwrap();
            db.execute("CREATE INDEX idx_player_uuid ON player_stats (uuid)").unwrap();
            db.execute("INSERT INTO player_stats VALUES ('player-y', 75)").unwrap();
            db.checkpoint().unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM player_stats WHERE uuid = 'player-y'").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], Value::Integer(75));
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.wal", path.display())).unwrap();
    }


    #[test]
    fn bulk_insert_updates_existing_index() {
        let mut db = Database::new();
        db.execute("CREATE TABLE players (uuid TEXT, coins INTEGER)").unwrap();
        db.execute("CREATE INDEX idx_uuid ON players (uuid)").unwrap();
        db.insert_many("players", vec![
            vec![Value::Text("bulk-a".into()), Value::Integer(1)],
            vec![Value::Text("bulk-b".into()), Value::Integer(2)],
        ]).unwrap();
        assert_eq!(db.execute("SELECT * FROM players WHERE uuid = 'bulk-b'").unwrap().rows.len(), 1);
    }


    #[test]
    fn prepared_parameters_are_typed_and_quoted_safely() {
        let mut db = Database::new();
        db.execute("CREATE TABLE messages (id INTEGER, body TEXT)").unwrap();
        db.execute_params(
            "INSERT INTO messages VALUES (?, ?)",
            &[Value::Integer(1), Value::Text("x'); DROP TABLE messages; --".into())],
        ).unwrap();
        let result = db.execute_params(
            "SELECT * FROM messages WHERE id = ?",
            &[Value::Integer(1)],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[1], Value::Text("x'); DROP TABLE messages; --".into()));
    }

    #[test]
    fn prepared_parameter_count_must_match() {
        let mut db = Database::new();
        assert!(db.execute_params("SELECT * FROM missing WHERE id = ?", &[]).is_err());
        assert!(db.execute_params("SELECT * FROM missing", &[Value::Integer(1)]).is_err());
    }


    #[test]
    fn wal_replay_restores_index_definitions() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-index-wal-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE indexed_players (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("INSERT INTO indexed_players VALUES ('player-1', 12)").unwrap();
            db.execute("CREATE INDEX idx_indexed_players_uuid ON indexed_players (uuid)").unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM indexed_players WHERE uuid = 'player-1'").unwrap();
        assert_eq!(result.rows.len(), 1);
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.wal", path.display())).unwrap();
    }


    #[test]
    fn owned_session_commits_atomically_and_detects_conflicts() {
        let mut db = Database::new();
        db.execute("CREATE TABLE sessions (value INTEGER)").unwrap();
        let mut first = db.begin_session();
        let mut second = db.begin_session();
        first.execute("INSERT INTO sessions VALUES (1)").unwrap();
        second.execute("INSERT INTO sessions VALUES (2)").unwrap();
        db.commit_session(first).unwrap();
        assert!(matches!(db.commit_session(second), Err(DbError::TransactionConflict)));
        assert_eq!(db.execute("SELECT * FROM sessions").unwrap().rows.len(), 1);
    }


    #[test]
    fn persistent_owned_session_commits_as_one_recoverable_unit() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-session-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            let mut session = db.begin_session();
            session.execute("CREATE TABLE session_rows (value INTEGER)").unwrap();
            session.execute_params("INSERT INTO session_rows VALUES (?)", &[Value::Integer(11)]).unwrap();
            assert_eq!(session.execute("SELECT * FROM session_rows").unwrap().rows.len(), 1);
            db.commit_session(session).unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        let rows = reopened.execute("SELECT * FROM session_rows").unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Integer(11));
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.wal", path.display())).unwrap();
    }


    #[test]
    fn sql_supports_column_inserts_projections_updates_and_deletes() {
        let mut db = Database::new();
        db.execute("CREATE TABLE players (uuid TEXT, coins INTEGER, active BOOLEAN)").unwrap();
        db.execute("INSERT INTO players (coins, uuid, active) VALUES (50, 'player-a', TRUE)").unwrap();
        db.execute("INSERT INTO players (uuid, coins, active) VALUES ('player-b', 75, FALSE)").unwrap();
        db.execute("CREATE INDEX idx_players_uuid ON players (uuid)").unwrap();

        let projected = db.execute("SELECT coins, uuid FROM players WHERE uuid = 'player-a'").unwrap();
        assert_eq!(projected.columns, vec!["coins", "uuid"]);
        assert_eq!(projected.rows[0].values, vec![Value::Integer(50), Value::Text("player-a".into())]);

        let updated = db.execute("UPDATE players SET coins = 99 WHERE uuid = 'player-a'").unwrap();
        assert_eq!(updated.affected_rows, 1);
        assert_eq!(db.execute("SELECT coins FROM players WHERE uuid = 'player-a'").unwrap().rows[0].values[0], Value::Integer(99));

        let deleted = db.execute("DELETE FROM players WHERE uuid = 'player-b'").unwrap();
        assert_eq!(deleted.affected_rows, 1);
        assert_eq!(db.execute("SELECT * FROM players").unwrap().rows.len(), 1);
    }

    #[test]
    fn persistent_update_and_delete_replay_from_wal() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-dml-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE dml_rows (id INTEGER, label TEXT)").unwrap();
            db.execute("INSERT INTO dml_rows VALUES (1, 'first')").unwrap();
            db.execute("INSERT INTO dml_rows VALUES (2, 'second')").unwrap();
            db.execute("UPDATE dml_rows SET label = 'changed' WHERE id = 1").unwrap();
            db.execute("DELETE FROM dml_rows WHERE id = 2").unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        let rows = reopened.execute("SELECT label FROM dml_rows WHERE id = 1").unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Text("changed".into()));
        assert_eq!(reopened.execute("SELECT * FROM dml_rows").unwrap().rows.len(), 1);
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.wal", path.display())).unwrap();
    }


    #[test]
    fn where_and_supports_multiple_parameters_without_splitting_quoted_text() {
        let mut db = Database::new();
        db.execute("CREATE TABLE ksl_persist (script_name TEXT, key TEXT, value TEXT)").unwrap();
        db.execute_params(
            "INSERT INTO ksl_persist VALUES (?, ?, ?)",
            &[Value::Text("economy".into()), Value::Text("message".into()), Value::Text("bread AND apples".into())],
        ).unwrap();
        db.execute_params(
            "INSERT INTO ksl_persist VALUES (?, ?, ?)",
            &[Value::Text("homes".into()), Value::Text("message".into()), Value::Text("other".into())],
        ).unwrap();
        let result = db.execute_params(
            "SELECT value FROM ksl_persist WHERE script_name = ? AND key = ?",
            &[Value::Text("economy".into()), Value::Text("message".into())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Text("bread AND apples".into()));
    }


    #[test]
    fn parses_ksl_style_table_migrations_and_column_lists() {
        let mut db = Database::new();
        db.execute("CREATE TABLE IF NOT EXISTS ksl_persist (script_name VARCHAR(190) NOT NULL, \"key\" VARCHAR(190) NOT NULL, value TEXT, PRIMARY KEY (script_name, \"key\"))").unwrap();
        db.execute("CREATE TABLE IF NOT EXISTS ksl_persist (script_name VARCHAR(190) NOT NULL, \"key\" VARCHAR(190) NOT NULL, value TEXT, PRIMARY KEY (script_name, \"key\"))").unwrap();
        db.execute_params(
            "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?)",
            &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("150".into())],
        ).unwrap();
        let result = db.execute_params(
            "SELECT value FROM ksl_persist WHERE script_name = ? AND \"key\" = ?",
            &[Value::Text("economy".into()), Value::Text("balance".into())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Text("150".into()));
        assert!(db.execute("INSERT INTO ksl_persist VALUES ('economy', 'balance', 'duplicate')").is_err());
    }


    #[test]
    fn supports_postgres_and_mariadb_upsert_syntax_for_ksl_keys() {
        let mut db = Database::new();
        db.execute("CREATE TABLE ksl_persist (script_name VARCHAR(190) NOT NULL, \"key\" VARCHAR(190) NOT NULL, value TEXT, PRIMARY KEY (script_name, \"key\"))").unwrap();
        db.execute_params(
            "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?)",
            &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("100".into())],
        ).unwrap();
        db.execute_params(
            "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?) ON CONFLICT (script_name, \"key\") DO UPDATE SET value = EXCLUDED.value",
            &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("250".into())],
        ).unwrap();
        let result = db.execute_params(
            "SELECT value FROM ksl_persist WHERE script_name = ? AND \"key\" = ?",
            &[Value::Text("economy".into()), Value::Text("balance".into())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Text("250".into()));

        db.execute_params(
            "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
            &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("300".into())],
        ).unwrap();
        let result = db.execute_params(
            "SELECT value FROM ksl_persist WHERE script_name = ? AND \"key\" = ?",
            &[Value::Text("economy".into()), Value::Text("balance".into())],
        ).unwrap();
        assert_eq!(result.rows[0].values[0], Value::Text("300".into()));
    }


    #[test]
    fn supports_count_star_with_where_predicates() {
        let mut db = Database::new();
        db.execute("CREATE TABLE player_notes (uuid TEXT, note TEXT)").unwrap();
        db.execute("INSERT INTO player_notes VALUES ('a', 'one')").unwrap();
        db.execute("INSERT INTO player_notes VALUES ('a', 'two')").unwrap();
        db.execute("INSERT INTO player_notes VALUES ('b', 'three')").unwrap();
        let result = db.execute("SELECT COUNT(*) AS total FROM player_notes WHERE uuid = 'a'").unwrap();
        assert_eq!(result.columns, vec!["total"]);
        assert_eq!(result.rows[0].values[0], Value::Integer(2));
    }


    #[test]
    fn persistent_ksl_composite_key_and_upsert_survive_checkpoint() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-ksl-pk-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE IF NOT EXISTS ksl_persist (script_name VARCHAR(190) NOT NULL, \"key\" VARCHAR(190) NOT NULL, value TEXT, PRIMARY KEY (script_name, \"key\"))").unwrap();
            db.execute_params(
                "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?) ON CONFLICT (script_name, \"key\") DO UPDATE SET value = EXCLUDED.value",
                &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("125".into())],
            ).unwrap();
            db.execute_params(
                "INSERT INTO ksl_persist (script_name, \"key\", value) VALUES (?, ?, ?) ON CONFLICT (script_name, \"key\") DO UPDATE SET value = EXCLUDED.value",
                &[Value::Text("economy".into()), Value::Text("balance".into()), Value::Text("400".into())],
            ).unwrap();
            db.checkpoint().unwrap();
        }
        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute_params(
            "SELECT value FROM ksl_persist WHERE script_name = ? AND \"key\" = ?",
            &[Value::Text("economy".into()), Value::Text("balance".into())],
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Text("400".into()));
        assert!(reopened.execute("INSERT INTO ksl_persist VALUES ('economy', 'balance', 'duplicate')").is_err());
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(format!("{}.wal", path.display())).unwrap();
    }


    #[test]
    fn durable_sql_commit_survives_embertree_mirror_failure() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-mirror-failure-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let page_path = format!("{}.pages", path.display());
        let wal_path = format!("{}.wal", path.display());
        let lock_path = format!("{}.lock", path.display());
        {
            let mut db = Database::open(&path).unwrap();
            db.execute("CREATE TABLE mirror_failure (id INTEGER, tag TEXT)").unwrap();
            db.execute("CREATE INDEX idx_mirror_failure_tag ON mirror_failure (tag)").unwrap();
            db.execute("INSERT INTO mirror_failure VALUES (5, 'seed')").unwrap();

            std::fs::remove_file(&page_path).unwrap();
            std::fs::create_dir(&page_path).unwrap();
            db.execute("INSERT INTO mirror_failure VALUES (41, 'committed')").unwrap();
            assert!(!db.ember_indexes_valid);
            let result = db.execute("SELECT * FROM mirror_failure WHERE tag = 'committed'").unwrap();
            assert_eq!(result.rows.len(), 1);
            assert_eq!(result.rows[0].values[0], Value::Integer(41));
        }
        std::fs::remove_dir(&page_path).unwrap();

        let mut reopened = Database::open(&path).unwrap();
        let result = reopened.execute("SELECT * FROM mirror_failure WHERE tag = 'committed'").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Integer(41));
        drop(reopened);

        for candidate in [path.to_string_lossy().to_string(), wal_path, lock_path, page_path, format!("{}.pages.btree.wal", path.display())] {
            let _ = std::fs::remove_file(candidate);
        }
    }

    #[test]
    fn select_limit_bounds_result_rows_with_and_without_index_predicates() {
        let mut db = Database::new();
        db.execute("CREATE TABLE player_events (uuid TEXT, event TEXT)").unwrap();
        db.execute("INSERT INTO player_events VALUES ('a', 'join')").unwrap();
        db.execute("INSERT INTO player_events VALUES ('a', 'move')").unwrap();
        db.execute("INSERT INTO player_events VALUES ('b', 'join')").unwrap();
        db.execute("CREATE INDEX idx_player_events_uuid ON player_events (uuid)").unwrap();

        assert_eq!(db.execute("SELECT * FROM player_events LIMIT 2").unwrap().rows.len(), 2);
        assert_eq!(db.execute("SELECT * FROM player_events WHERE uuid = 'a' LIMIT 1").unwrap().rows.len(), 1);
        assert_eq!(db.execute("SELECT * FROM player_events LIMIT 0").unwrap().rows.len(), 0);
        assert!(db.execute("SELECT * FROM player_events LIMIT nope").is_err());
        assert_eq!(db.execute("SELECT COUNT(*) FROM player_events LIMIT 1").unwrap().rows[0].values[0], Value::Integer(3));
    }


    #[test]
    fn persistent_database_uses_exclusive_process_lock() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-lock-{}-{}.pdb",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let first = Database::open(&path).unwrap();
        assert!(Database::open(&path).is_err(), "a second open must not acquire the same database");
        drop(first);
        let second = Database::open(&path).unwrap();
        drop(second);
        std::fs::remove_file(&path).unwrap();
        let wal_path = format!("{}.wal", path.display());
        if std::path::Path::new(&wal_path).exists() {
            std::fs::remove_file(wal_path).unwrap();
        }
        std::fs::remove_file(format!("{}.lock", path.display())).unwrap();
    }

    #[test]
    fn rejects_wrong_types() {
        let mut db = Database::new();
        db.execute("CREATE TABLE metrics (value INTEGER)").unwrap();
        assert!(db.execute("INSERT INTO metrics VALUES ('wrong')").is_err());
    }
    #[test]
    fn in_memory_indexes_store_stable_row_ids_and_update_incrementally() {
        let mut db = Database::new();
        db.execute("CREATE TABLE indexed_events (player TEXT, bucket TEXT, score INTEGER)").unwrap();
        db.execute("INSERT INTO indexed_events VALUES ('a', 'hot', 1)").unwrap();
        db.execute("INSERT INTO indexed_events VALUES ('b', 'hot', 2)").unwrap();
        db.execute("INSERT INTO indexed_events VALUES ('c', 'hot', 3)").unwrap();
        db.execute("CREATE INDEX idx_events_bucket ON indexed_events (bucket)").unwrap();

        let original = db.execute("SELECT * FROM indexed_events WHERE bucket = 'hot'").unwrap()
            .rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>();
        assert_eq!(original, vec![1, 2, 3]);
        {
            let table = db.tables.get("indexed_events").unwrap();
            let key = IndexKey::from(&Value::Text("hot".into()));
            assert_eq!(
                table.indexes.get("idx_events_bucket").unwrap().entries.get(&key).unwrap(),
                &vec![1, 2, 3],
                "the in-memory index stores stable row IDs, not vector offsets"
            );
        }

        db.execute("DELETE FROM indexed_events WHERE player = 'a'").unwrap();
        let remaining = db.execute("SELECT * FROM indexed_events WHERE bucket = 'hot'").unwrap()
            .rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>();
        assert_eq!(remaining, vec![2, 3], "compaction must not invalidate index references");

        db.execute("UPDATE indexed_events SET bucket = 'cold' WHERE player = 'c'").unwrap();
        assert_eq!(db.execute("SELECT * FROM indexed_events WHERE bucket = 'hot'").unwrap()
            .rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>(), vec![2]);
        assert_eq!(db.execute("SELECT * FROM indexed_events WHERE bucket = 'cold'").unwrap()
            .rows.into_iter().map(|row| row.row_id).collect::<Vec<_>>(), vec![3]);

        db.execute("UPDATE indexed_events SET score = 99 WHERE player = 'b'").unwrap();
        assert_eq!(db.execute("SELECT score FROM indexed_events WHERE bucket = 'hot'").unwrap().rows[0].values[0], Value::Integer(99));
    }

    #[test]
    fn indexed_upsert_rekeys_only_the_affected_secondary_entry() {
        let mut db = Database::new();
        db.execute("CREATE TABLE indexed_upserts (sku TEXT PRIMARY KEY, category TEXT, quantity INTEGER)").unwrap();
        db.execute("CREATE INDEX idx_upserts_category ON indexed_upserts (category)").unwrap();
        db.execute("INSERT INTO indexed_upserts VALUES ('book', 'old', 1)").unwrap();
        let original_id = db.execute("SELECT * FROM indexed_upserts WHERE sku = 'book'").unwrap().rows[0].row_id;

        db.execute("INSERT INTO indexed_upserts VALUES ('book', 'new', 5) ON CONFLICT (sku) DO UPDATE SET category = EXCLUDED.category, quantity = EXCLUDED.quantity").unwrap();

        assert!(db.execute("SELECT * FROM indexed_upserts WHERE category = 'old'").unwrap().rows.is_empty());
        let updated = db.execute("SELECT * FROM indexed_upserts WHERE category = 'new'").unwrap();
        assert_eq!(updated.rows.len(), 1);
        assert_eq!(updated.rows[0].row_id, original_id);
        assert_eq!(updated.rows[0].values[2], Value::Integer(5));
    }

    #[test]
    fn unique_index_rejects_bulk_update_duplicates_atomically() {
        let mut db = Database::new();
        db.execute("CREATE TABLE unique_updates (group_name TEXT, email TEXT, PRIMARY KEY (email))").unwrap();
        db.execute("INSERT INTO unique_updates VALUES ('team', 'a@example.test')").unwrap();
        db.execute("INSERT INTO unique_updates VALUES ('team', 'b@example.test')").unwrap();

        assert!(db.execute("UPDATE unique_updates SET email = 'same@example.test' WHERE group_name = 'team'").is_err());
        let rows = db.execute("SELECT * FROM unique_updates").unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(rows.rows[0].values[1], Value::Text("a@example.test".into()));
        assert_eq!(rows.rows[1].values[1], Value::Text("b@example.test".into()));
    }

}
