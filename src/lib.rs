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
pub mod client;

pub use kv::{KeyValueStore, Registry, RegistryValue};
pub use auth::{AuditEvent, AuthStore, Permission, Permissions, Principal, RoleInfo, UserInfo};
pub use client::{RemoteClient, RemoteServerStatus, RemoteTablePage};
pub use storage::record_store::RecordStore;
pub use storage::sql_rows::SqlRowStore;
pub use storage::transaction_coordinator::{CoordinatorParticipant, RecoveredTransaction, TransactionCoordinator, TransactionState};
pub use storage::emberforge7::{EmberForge7Namespace, EmberForge7NamespaceKind, EmberForge7PlayerProfile, EmberForge7Stats, EmberForge7Store};

use sql::{
    apply_upsert, bind_parameters, find_kw, find_unquoted_keyword, normalize_ident,
    parse_order_by, parse_value, parse_where_expression,
    split_csv, starts_with_kw, take_parenthesized, value_to_sql, validate_value,
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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Row {
    /// Stable table-local identifier, independent of the current vector position.
    pub row_id: u64,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Read-only view of the SQL catalog, suitable for database administration tools.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CatalogSnapshot {
    pub tables: Vec<TableInfo>,
}

/// Metadata for one SQL table. Rows are not copied into the catalog snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub row_count: usize,
    pub columns: Vec<Column>,
    pub indexes: Vec<IndexInfo>,
}

/// Metadata for one index attached to a SQL table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComparisonOperator {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
    Between,
    NotBetween,
    IsNull,
    IsNotNull,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SqlExpression {
    True,
    Predicate(SqlPredicate),
    And(Vec<SqlExpression>),
    Or(Vec<SqlExpression>),
    Not(Box<SqlExpression>),
}

impl Default for SqlExpression {
    fn default() -> Self { Self::True }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SqlPredicate {
    pub column_index: usize,
    pub operator: ComparisonOperator,
    pub value: Option<Value>,
    /// Additional operands used by IN and BETWEEN predicates.
    pub values: Vec<Value>,
    /// OR-group number; predicates inside one group are ANDed.
    pub group: usize,
}

fn sql_like_matches(value: &str, pattern: &str) -> bool {
    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; value.len() + 1];
        match token {
            '%' => {
                current[0] = previous[0];
                for index in 1..=value.len() {
                    current[index] = previous[index] || current[index - 1];
                }
            }
            '_' => {
                for index in 1..=value.len() {
                    current[index] = previous[index - 1];
                }
            }
            literal => {
                for index in 1..=value.len() {
                    current[index] = previous[index - 1] && value[index - 1] == literal;
                }
            }
        }
        previous = current;
    }
    previous[value.len()]
}

fn estimate_row_values_heap_bytes(values: &[Value]) -> usize {
    std::mem::size_of_val(values).saturating_add(values.iter().fold(0usize, |total, value| {
        let owned_bytes = match value {
            Value::Text(text) => text.capacity(),
            Value::Null | Value::Integer(_) | Value::Boolean(_) => 0,
        };
        total.saturating_add(owned_bytes)
    }))
}

fn evaluate_sql_expression(values: &[Value], expression: &SqlExpression) -> Option<bool> {
    match expression {
        SqlExpression::True => Some(true),
        SqlExpression::Predicate(predicate) => {
            let value = values.get(predicate.column_index)?;
            match predicate.operator {
                ComparisonOperator::IsNull => Some(matches!(value, Value::Null)),
                ComparisonOperator::IsNotNull => Some(!matches!(value, Value::Null)),
                ComparisonOperator::IsDistinctFrom => {
                    let expected = predicate.value.as_ref()?;
                    Some(match (value, expected) {
                        (Value::Null, Value::Null) => false,
                        (Value::Null, _) | (_, Value::Null) => true,
                        _ => compare_sql_values(value, expected) != std::cmp::Ordering::Equal,
                    })
                }
                ComparisonOperator::IsNotDistinctFrom => {
                    let expected = predicate.value.as_ref()?;
                    Some(match (value, expected) {
                        (Value::Null, Value::Null) => true,
                        (Value::Null, _) | (_, Value::Null) => false,
                        _ => compare_sql_values(value, expected) == std::cmp::Ordering::Equal,
                    })
                }
                ComparisonOperator::Like | ComparisonOperator::NotLike => {
                    let (Value::Text(actual), Some(Value::Text(pattern))) = (value, predicate.value.as_ref()) else {
                        return None;
                    };
                    let matched = sql_like_matches(actual, pattern);
                    Some(if predicate.operator == ComparisonOperator::NotLike { !matched } else { matched })
                }
                ComparisonOperator::In | ComparisonOperator::NotIn => {
                    if matches!(value, Value::Null) { return None; }
                    let mut saw_null = false;
                    let mut found = false;
                    for candidate in &predicate.values {
                        if matches!(candidate, Value::Null) {
                            saw_null = true;
                        } else if compare_sql_values(value, candidate) == std::cmp::Ordering::Equal {
                            found = true;
                            break;
                        }
                    }
                    if predicate.operator == ComparisonOperator::In {
                        if found { Some(true) } else if saw_null { None } else { Some(false) }
                    } else if found {
                        Some(false)
                    } else if saw_null {
                        None
                    } else {
                        Some(true)
                    }
                }
                ComparisonOperator::Between | ComparisonOperator::NotBetween => {
                    if matches!(value, Value::Null) { return None; }
                    let (Some(lower), Some(upper)) = (predicate.values.first(), predicate.values.get(1)) else {
                        return None;
                    };
                    if matches!(lower, Value::Null) || matches!(upper, Value::Null) { return None; }
                    let inside = compare_sql_values(value, lower) != std::cmp::Ordering::Less
                        && compare_sql_values(value, upper) != std::cmp::Ordering::Greater;
                    Some(if predicate.operator == ComparisonOperator::NotBetween { !inside } else { inside })
                }
                operator => {
                    let expected = predicate.value.as_ref()?;
                    if matches!(value, Value::Null) || matches!(expected, Value::Null) { return None; }
                    let ordering = compare_sql_values(value, expected);
                    Some(match operator {
                        ComparisonOperator::Equal => ordering == std::cmp::Ordering::Equal,
                        ComparisonOperator::NotEqual => ordering != std::cmp::Ordering::Equal,
                        ComparisonOperator::Less => ordering == std::cmp::Ordering::Less,
                        ComparisonOperator::LessOrEqual => ordering != std::cmp::Ordering::Greater,
                        ComparisonOperator::Greater => ordering == std::cmp::Ordering::Greater,
                        ComparisonOperator::GreaterOrEqual => ordering != std::cmp::Ordering::Less,
                        _ => false,
                    })
                }
            }
        }
        SqlExpression::And(expressions) => {
            let mut unknown = false;
            for item in expressions {
                match evaluate_sql_expression(values, item) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(true) }
        }
        SqlExpression::Or(expressions) => {
            let mut unknown = false;
            for item in expressions {
                match evaluate_sql_expression(values, item) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown { None } else { Some(false) }
        }
        SqlExpression::Not(inner) => evaluate_sql_expression(values, inner).map(|value| !value),
    }
}

fn matches_sql_expression(values: &[Value], expression: &SqlExpression) -> bool {
    evaluate_sql_expression(values, expression) == Some(true)
}

fn sql_expression_equality_terms(expression: &SqlExpression) -> Option<Vec<(usize, Value)>> {
    match expression {
        SqlExpression::Predicate(predicate)
            if predicate.operator == ComparisonOperator::Equal
                && predicate.values.is_empty()
                && predicate.value.as_ref().is_some_and(|value| !matches!(value, Value::Null)) =>
        {
            Some(vec![(predicate.column_index, predicate.value.clone()?)])
        }
        SqlExpression::And(expressions) => {
            let mut terms = Vec::new();
            for expression in expressions {
                terms.extend(sql_expression_equality_terms(expression)?);
            }
            (!terms.is_empty()).then_some(terms)
        }
        _ => None,
    }
}

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

fn compare_sql_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
        (Value::Integer(_), _) => Ordering::Less,
        (Value::Text(_), Value::Boolean(_)) => Ordering::Less,
        (Value::Text(_), Value::Integer(_)) => Ordering::Greater,
        (Value::Boolean(_), _) => Ordering::Greater,
    }
}

fn compare_rows_by_order(left: &Row, right: &Row, order: &[(usize, bool)]) -> std::cmp::Ordering {
    for (column, descending) in order {
        let ordering = compare_sql_values(&left.values[*column], &right.values[*column]);
        if ordering != std::cmp::Ordering::Equal {
            return if *descending { ordering.reverse() } else { ordering };
        }
    }
    std::cmp::Ordering::Equal
}

#[derive(Debug, Clone)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>,
    /// Durable row count retained when the PDB6 row cache is not resident.
    persisted_row_count: usize,
    /// False means row payloads must be fetched from PDB6 before a write transaction.
    rows_loaded: bool,
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

fn describe_table(name: &str, table: &Table) -> TableInfo {
    let indexes = table.indexes.iter().map(|(index_name, index)| IndexInfo {
        name: index_name.clone(),
        columns: index.column_indices.iter()
            .filter_map(|column_index| table.columns.get(*column_index).map(|column| column.name.clone()))
            .collect(),
        unique: index.unique,
    }).collect();

    TableInfo {
        name: name.to_owned(),
        row_count: if table.rows_loaded { table.rows.len() } else { table.persisted_row_count },
        columns: table.columns.clone(),
        indexes,
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

#[derive(Debug)]
enum Pdb6DmlOperation {
    Insert { columns: Option<Vec<String>>, supplied: Vec<Value> },
    Update { column: String, value: Value, predicate: Option<String> },
    Delete { predicate: Option<String> },
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

    /// Returns a stable, read-only snapshot of SQL table and index metadata.
    ///
    /// The snapshot contains metadata only; it does not clone table row payloads.
    pub fn catalog(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            tables: self.tables.iter()
                .map(|(name, table)| describe_table(name, table))
                .collect(),
        }
    }

    /// Returns metadata for a single table using the same identifier rules as SQL.
    pub fn table_info(&self, table_name: &str) -> Result<TableInfo, DbError> {
        let name = normalize_ident(table_name);
        let table = self.tables.get(&name)
            .ok_or_else(|| DbError::TableNotFound(name.clone()))?;
        Ok(describe_table(&name, table))
    }

    /// Reads a bounded page of rows without materializing the entire table.
    ///
    /// Pages follow the table's current physical row order. The maximum page
    /// size is capped to keep administration tools from accidentally loading
    /// an unbounded result set into memory.
    pub fn read_table_page(
        &self,
        table_name: &str,
        offset: usize,
        limit: usize,
    ) -> Result<QueryResult, DbError> {
        const MAX_PAGE_SIZE: usize = 1_000;
        if limit > MAX_PAGE_SIZE {
            return Err(DbError::Constraint(format!(
                "table page size exceeds the maximum of {MAX_PAGE_SIZE} rows"
            )));
        }
        let name = normalize_ident(table_name);
        let table = self.tables.get(&name)
            .ok_or_else(|| DbError::TableNotFound(name.clone()))?;
        if self.pdb6_mode {
            let path = self.storage_path.as_deref()
                .ok_or_else(|| DbError::Storage("PDB6 database has no storage path".into()))?;
            let mut rows = Vec::with_capacity(limit);
            let mut skipped = 0usize;
            storage::scan_pdb6_table_rows(path, &name, 512, |row| {
                if skipped < offset {
                    skipped += 1;
                    return Ok(true);
                }
                if rows.len() >= limit {
                    return Ok(false);
                }
                rows.push(row);
                Ok(rows.len() < limit)
            })?;
            return Ok(QueryResult {
                columns: table.columns.iter().map(|column| column.name.clone()).collect(),
                message: format!("{} row(s) in page (offset {offset})", rows.len()),
                rows,
                affected_rows: 0,
            });
        }
        let page = table.rows.get(offset..).unwrap_or(&[]);
        let rows = page.iter()
            .take(limit)
            .map(|row| Row { row_id: row.row_id, values: row.values.clone() })
            .collect::<Vec<_>>();
        Ok(QueryResult {
            columns: table.columns.iter().map(|column| column.name.clone()).collect(),
            message: format!("{} row(s) in page (offset {offset})", rows.len()),
            rows,
            affected_rows: 0,
        })
    }

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

    fn hydrate_pdb6_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if !self.pdb6_mode { return Ok(()); }
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 database has no storage path".into()))?;
        if let Some(table) = self.tables.get_mut(table_name) {
            storage::hydrate_pdb6_table(path, table_name, table)?;
        }
        Ok(())
    }

    /// Loads only pre-existing tables whose staged image was actually needed by a write.
    fn hydrate_pdb6_changed_tables(&mut self, staged: &Database) -> Result<(), DbError> {
        if !self.pdb6_mode { return Ok(()); }
        let changed = staged.tables.iter().filter_map(|(name, staged_table)| {
            let original_is_cold = self.tables.get(name).is_some_and(|table| !table.rows_loaded);
            (staged_table.rows_loaded && original_is_cold).then(|| name.clone())
        }).collect::<Vec<_>>();
        for table_name in changed {
            self.hydrate_pdb6_table(&table_name)?;
        }
        Ok(())
    }

    fn evict_pdb6_row_cache(&mut self) {
        if !self.pdb6_mode { return; }
        for table in self.tables.values_mut() {
            table.rows.clear();
            table.row_positions.clear();
            table.rows_loaded = false;
            for index in table.indexes.values_mut() {
                index.entries.clear();
            }
        }
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
        if self.pdb6_mode
            && (starts_with_kw(statement, "INSERT INTO")
                || starts_with_kw(statement, "UPDATE")
                || starts_with_kw(statement, "DELETE FROM"))
            && !statement.to_ascii_uppercase().contains("ON CONFLICT")
            && !statement.to_ascii_uppercase().contains("ON DUPLICATE KEY")
        {
            return self.execute_pdb6_bounded_dml(statement);
        }
        if is_write {
            return self.transaction(|transaction| transaction.execute(statement));
        }
        self.execute_inner(statement)
    }

    /// Executes one PDB6 DML statement with bounded row staging.
    ///
    /// Only rows matched by UPDATE/DELETE are retained in memory (up to the
    /// storage transaction limit). Row payloads are read in stable-ID batches;
    /// row descriptors, index entries, catalog counters and the manifest are
    /// then published through one copy-on-write root.
    fn execute_pdb6_bounded_dml(&mut self, statement: &str) -> Result<QueryResult, DbError> {
        const SCAN_BATCH: usize = 512;
        const MAX_CHANGED_ROWS: usize = 5_000;
        // Bound decoded row values retained by one statement independently of
        // the row-count limit. Large TEXT rows must not turn a 5,000-row cap
        // into an effectively unbounded allocation.
        const MAX_STAGED_ROW_BYTES: usize = 128 * 1024 * 1024;

        self.ensure_no_prepared_sql_transactions()?;
        let path = self.storage_path.clone()
            .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
        let generation = self.wal_sequence.checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;

        let (table_name, operation) = if starts_with_kw(statement, "INSERT INTO") {
            let rest = statement["INSERT INTO".len()..].trim();
            let values_pos = find_kw(rest, "VALUES")
                .ok_or_else(|| DbError::Parse("expected VALUES".into()))?;
            let target = rest[..values_pos].trim();
            let (name, columns) = if let Some(open) = target.find('(') {
                let (columns_text, _) = take_parenthesized(&target[open..])?;
                (normalize_ident(target[..open].trim()), Some(
                    split_csv(columns_text).into_iter().map(normalize_ident).collect::<Vec<_>>()
                ))
            } else {
                (normalize_ident(target), None)
            };
            let (values_text, upsert_clause) = take_parenthesized(rest[values_pos + "VALUES".len()..].trim())?;
            if !upsert_clause.trim().is_empty() {
                return Err(DbError::Unsupported(
                    "PDB6 bounded DML currently requires plain INSERT; UPSERT uses the compatibility transaction path".into()
                ));
            }
            let supplied = split_csv(values_text).into_iter().map(parse_value).collect::<Result<Vec<_>, _>>()?;
            (name, Pdb6DmlOperation::Insert { columns, supplied })
        } else if starts_with_kw(statement, "UPDATE") {
            let rest = statement["UPDATE".len()..].trim();
            let set_pos = find_kw(rest, "SET")
                .ok_or_else(|| DbError::Parse("expected SET in UPDATE".into()))?;
            let name = normalize_ident(rest[..set_pos].trim());
            let after_set = rest[set_pos + 3..].trim();
            let where_pos = find_unquoted_keyword(after_set, " WHERE ");
            let (assignment, predicate) = match where_pos {
                Some(position) => (&after_set[..position], Some(after_set[position + 7..].trim())),
                None => (after_set, None),
            };
            let equals = assignment.find('=')
                .ok_or_else(|| DbError::Parse("expected '=' in UPDATE assignment".into()))?;
            let column = normalize_ident(assignment[..equals].trim());
            let value = parse_value(assignment[equals + 1..].trim())?;
            (name, Pdb6DmlOperation::Update { column, value, predicate: predicate.map(str::to_owned) })
        } else {
            let rest = statement["DELETE FROM".len()..].trim();
            let where_pos = find_unquoted_keyword(rest, " WHERE ");
            let (table_part, predicate) = match where_pos {
                Some(position) => (&rest[..position], Some(rest[position + 7..].trim())),
                None => (rest, None),
            };
            (normalize_ident(table_part), Pdb6DmlOperation::Delete {
                predicate: predicate.map(str::to_owned)
            })
        };

        let table_schema = self.tables.get(&table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.clone()))?.clone();
        if table_schema.rows_loaded {
            return Err(DbError::Storage(
                "PDB6 bounded DML expected a cold table cache; refusing an unsafe mixed cache write".into()
            ));
        }

        let mut store = storage::pdb6_store::Pdb6Store::open(&path, 128)?;
        let mut manifest = store.manifest()?
            .ok_or_else(|| DbError::Storage("PDB6 SQL manifest is missing".into()))?;
        let mut catalog = None;
        for table_id in 1..=u64::from(manifest.table_count) {
            let encoded = store.get(&storage::pdb6::table_key(table_id)?)?
                .ok_or_else(|| DbError::Storage(format!("PDB6 manifest references missing table {table_id}")))?;
            let candidate = storage::pdb6::CatalogTable::decode(&encoded)?;
            if candidate.name == table_name {
                catalog = Some(candidate);
                break;
            }
        }
        let mut catalog = catalog.ok_or_else(|| DbError::Storage(format!(
            "PDB6 catalog does not contain table {table_name}"
        )))?;
        let table_id = catalog.table_id;
        if catalog.name != table_name
            || catalog.columns.len() != table_schema.columns.len()
            || catalog.indexes.len() != table_schema.indexes.len()
            || catalog.columns.iter().zip(&table_schema.columns).any(|(stored, memory)| {
                stored.name != memory.name
                    || stored.data_type != memory.data_type
                    || stored.nullable != memory.nullable
            })
        {
            return Err(DbError::Storage("PDB6 catalog and in-memory SQL schema disagree".into()));
        }
        for (stored, (name, index)) in catalog.indexes.iter().zip(table_schema.indexes.iter()) {
            let columns = index.column_indices.iter()
                .map(|column| u32::try_from(*column))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DbError::Storage("PDB6 index column ordinal exceeds u32".into()))?;
            if stored.name != *name || stored.columns != columns || stored.unique != index.unique {
                return Err(DbError::Storage("PDB6 catalog and in-memory SQL index definitions disagree".into()));
            }
        }

        let index_definitions = table_schema.indexes.iter().enumerate()
            .map(|(offset, (name, index))| (name.clone(), (offset as u64) + 1, index.column_indices.clone(), index.unique))
            .collect::<Vec<_>>();

        let mut row_upserts = Vec::<(u64, u64, Vec<u8>)>::new();
        let mut row_removals = Vec::<Vec<u8>>::new();
        let mut index_upserts = Vec::<(Vec<u8>, Vec<u8>)>::new();
        let mut index_removals = Vec::<Vec<u8>>::new();
        let affected_rows;

        match &operation {
            Pdb6DmlOperation::Insert { columns, supplied } => {
                let values = if let Some(names) = columns {
                    if names.len() != supplied.len() {
                        return Err(DbError::Constraint(format!(
                            "{} insert columns but {} values", names.len(), supplied.len()
                        )));
                    }
                    let mut row = vec![Value::Null; table_schema.columns.len()];
                    let mut assigned = std::collections::BTreeSet::new();
                    for (name, value) in names.iter().zip(supplied.iter()) {
                        let index = table_schema.columns.iter().position(|column| column.name == *name)
                            .ok_or_else(|| DbError::ColumnNotFound(name.clone()))?;
                        if !assigned.insert(index) {
                            return Err(DbError::Constraint(format!("column listed more than once: {name}")));
                        }
                        row[index] = value.clone();
                    }
                    row
                } else {
                    if supplied.len() != table_schema.columns.len() {
                        return Err(DbError::Constraint(format!(
                            "expected {} values, received {}", table_schema.columns.len(), supplied.len()
                        )));
                    }
                    supplied.clone()
                };
                for (column, value) in table_schema.columns.iter().zip(&values) {
                    validate_value(column, value)?;
                }
                for (_, index_id, columns, unique) in &index_definitions {
                    if !unique { continue; }
                    let encoded = storage::index_codec::encode_index_key(
                        &IndexKey::from_columns(&values, columns)
                    )?;
                    let prefix = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                    if !store.scan_prefix(&prefix, 1)?.is_empty() {
                        return Err(DbError::Constraint("duplicate key for unique SQL index".into()));
                    }
                }
                let row_id = catalog.next_row_id;
                catalog.next_row_id = catalog.next_row_id.checked_add(1)
                    .ok_or_else(|| DbError::Storage("table row identifier space exhausted".into()))?;
                let payload = storage::sql_rows::SqlRowStore::encode_values(&values, table_schema.columns.len())?;
                row_upserts.push((table_id, row_id, payload));
                for (_, index_id, columns, _) in &index_definitions {
                    let encoded = storage::index_codec::encode_index_key(
                        &IndexKey::from_columns(&values, columns)
                    )?;
                    let mut key = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                    key.extend_from_slice(&row_id.to_be_bytes());
                    if key.len() > 1_024 {
                        return Err(DbError::Constraint("PDB6 index key exceeds 1,024 bytes".into()));
                    }
                    index_upserts.push((key, Vec::new()));
                }
                affected_rows = 1;
            }
            Pdb6DmlOperation::Update { column, value, predicate } => {
                let target_index = table_schema.columns.iter().position(|item| item.name == *column)
                    .ok_or_else(|| DbError::ColumnNotFound(column.clone()))?;
                validate_value(&table_schema.columns[target_index], value)?;
                let predicates = predicate.as_deref()
                    .map(|text| parse_where_expression(&table_schema.columns, text))
                    .transpose()?.unwrap_or_default();
                let mut changed = Vec::<(Row, Vec<Value>)>::new();
                let mut staged_row_bytes = 0usize;
                let mut matched_count = 0usize;
                let mut after = None;
                loop {
                    let ids = store.scan_row_ids(table_id, after, SCAN_BATCH)?;
                    if ids.is_empty() { break; }
                    for row_id in &ids {
                        let payload = store.get_row_payload(table_id, *row_id)?
                            .ok_or_else(|| DbError::Storage(format!("PDB6 row descriptor missing for row {row_id}")))?;
                        let values = storage::sql_rows::SqlRowStore::decode_values(&payload, table_schema.columns.len())?;
                        for (column, value) in table_schema.columns.iter().zip(&values) {
                            validate_value(column, value)?;
                        }
                        let matches = matches_sql_expression(&values, &predicates);
                        if matches {
                            matched_count = matched_count.checked_add(1)
                                .ok_or_else(|| DbError::Storage("SQL UPDATE match count overflow".into()))?;
                        }
                        if matches && values[target_index] != *value {
                            if changed.len() == MAX_CHANGED_ROWS {
                                return Err(DbError::Constraint(format!(
                                    "PDB6 bounded UPDATE matches more than {MAX_CHANGED_ROWS} changed rows; split it into smaller statements"
                                )));
                            }
                            let old_row_bytes = estimate_row_values_heap_bytes(&values);
                            let replaced_value_bytes = match &values[target_index] {
                                Value::Text(text) => text.capacity(),
                                Value::Null | Value::Integer(_) | Value::Boolean(_) => 0,
                            };
                            let replacement_value_bytes = match value {
                                Value::Text(text) => text.capacity(),
                                Value::Null | Value::Integer(_) | Value::Boolean(_) => 0,
                            };
                            let retained_bytes = old_row_bytes.saturating_mul(2)
                                .saturating_sub(replaced_value_bytes)
                                .saturating_add(replacement_value_bytes);
                            staged_row_bytes = staged_row_bytes.checked_add(retained_bytes)
                                .ok_or_else(|| DbError::Storage("SQL UPDATE staging byte count overflow".into()))?;
                            if staged_row_bytes > MAX_STAGED_ROW_BYTES {
                                return Err(DbError::Constraint(format!(
                                    "PDB6 bounded UPDATE exceeds the {MAX_STAGED_ROW_BYTES}-byte staging budget; split it into smaller statements"
                                )));
                            }
                            let mut new_values = values.clone();
                            new_values[target_index] = (*value).clone();
                            changed.push((Row { row_id: *row_id, values: new_values }, values));
                        }
                    }
                    after = ids.last().copied();
                    if ids.len() < SCAN_BATCH { break; }
                }
                let changing_ids = changed.iter().map(|(row, _)| row.row_id).collect::<std::collections::BTreeSet<_>>();
                for (_, index_id, columns, unique) in &index_definitions {
                    if !unique || !columns.contains(&target_index) { continue; }
                    let mut seen = std::collections::BTreeSet::new();
                    for (row, _) in &changed {
                        let encoded = storage::index_codec::encode_index_key(
                            &IndexKey::from_columns(&row.values, columns)
                        )?;
                        if !seen.insert(encoded.clone()) {
                            return Err(DbError::Constraint("UPDATE would create duplicate values in a unique index".into()));
                        }
                        let prefix = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                        for (key, _) in store.scan_prefix(&prefix, 5_000)? {
                            if key.len() < 8 {
                                return Err(DbError::Storage("invalid PDB6 SQL index row reference".into()));
                            }
                            let existing_id = u64::from_be_bytes(key[key.len()-8..].try_into().unwrap());
                            if !changing_ids.contains(&existing_id) {
                                return Err(DbError::Constraint("UPDATE conflicts with an existing unique index value".into()));
                            }
                        }
                    }
                }
                for (row, old_values) in &changed {
                    row_upserts.push((table_id, row.row_id,
                        storage::sql_rows::SqlRowStore::encode_values(&row.values, table_schema.columns.len())?));
                    for (_, index_id, columns, _) in &index_definitions {
                        let old_key = storage::index_codec::encode_index_key(
                            &IndexKey::from_columns(old_values, columns)
                        )?;
                        let new_key = storage::index_codec::encode_index_key(
                            &IndexKey::from_columns(&row.values, columns)
                        )?;
                        if old_key == new_key { continue; }
                        let mut old_key = storage::pdb6::index_key(table_id, *index_id, &old_key)?;
                        old_key.extend_from_slice(&row.row_id.to_be_bytes());
                        index_removals.push(old_key);
                        let mut new_key = storage::pdb6::index_key(table_id, *index_id, &new_key)?;
                        new_key.extend_from_slice(&row.row_id.to_be_bytes());
                        if new_key.len() > 1_024 {
                            return Err(DbError::Constraint("PDB6 index key exceeds 1,024 bytes".into()));
                        }
                        index_upserts.push((new_key, Vec::new()));
                    }
                }
                affected_rows = matched_count;
            }
            Pdb6DmlOperation::Delete { predicate } => {
                let predicates = predicate.as_deref()
                    .map(|text| parse_where_expression(&table_schema.columns, text))
                    .transpose()?.unwrap_or_default();
                let mut removed = Vec::<Row>::new();
                let mut staged_row_bytes = 0usize;
                let mut after = None;
                loop {
                    let ids = store.scan_row_ids(table_id, after, SCAN_BATCH)?;
                    if ids.is_empty() { break; }
                    for row_id in &ids {
                        let payload = store.get_row_payload(table_id, *row_id)?
                            .ok_or_else(|| DbError::Storage(format!("PDB6 row descriptor missing for row {row_id}")))?;
                        let values = storage::sql_rows::SqlRowStore::decode_values(&payload, table_schema.columns.len())?;
                        if matches_sql_expression(&values, &predicates) {
                            if removed.len() == MAX_CHANGED_ROWS {
                                return Err(DbError::Constraint(format!(
                                    "PDB6 bounded DELETE matches more than {MAX_CHANGED_ROWS} rows; split it into smaller statements"
                                )));
                            }
                            staged_row_bytes = staged_row_bytes.checked_add(estimate_row_values_heap_bytes(&values))
                                .ok_or_else(|| DbError::Storage("SQL DELETE staging byte count overflow".into()))?;
                            if staged_row_bytes > MAX_STAGED_ROW_BYTES {
                                return Err(DbError::Constraint(format!(
                                    "PDB6 bounded DELETE exceeds the {MAX_STAGED_ROW_BYTES}-byte staging budget; split it into smaller statements"
                                )));
                            }
                            removed.push(Row { row_id: *row_id, values });
                        }
                    }
                    after = ids.last().copied();
                    if ids.len() < SCAN_BATCH { break; }
                }
                for row in &removed {
                    row_removals.push(storage::pdb6::row_key(table_id, row.row_id)?);
                    for (_, index_id, columns, _) in &index_definitions {
                        let encoded = storage::index_codec::encode_index_key(
                            &IndexKey::from_columns(&row.values, columns)
                        )?;
                        let mut key = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                        key.extend_from_slice(&row.row_id.to_be_bytes());
                        index_removals.push(key);
                    }
                }
                affected_rows = removed.len();
            }
        }

        if affected_rows == 0 || (row_upserts.is_empty() && row_removals.is_empty()) {
            let message = if starts_with_kw(statement, "UPDATE") {
                format!("{affected_rows} row(s) updated")
            } else if starts_with_kw(statement, "DELETE FROM") {
                format!("{affected_rows} row(s) deleted")
            } else {
                "0 row(s) affected".into()
            };
            return Ok(QueryResult {
                columns: Vec::new(), rows: Vec::new(), affected_rows, message,
            });
        }

        manifest.generation = generation;
        manifest.last_tx_id = manifest.last_tx_id.checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 transaction ID space exhausted".into()))?;
        let catalog_bytes = catalog.encode()?;
        let root_bytes = manifest.encode()?;
        let mut metadata_upserts = vec![
            (storage::pdb6::table_key(table_id)?, catalog_bytes),
            (storage::pdb6::database_root_key(), root_bytes),
        ];
        metadata_upserts.append(&mut index_upserts);
        store.apply_row_batch(row_upserts, metadata_upserts, row_removals.into_iter().chain(index_removals))?;

        let table = self.tables.get_mut(&table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        table.next_row_id = catalog.next_row_id;
        match &operation {
            Pdb6DmlOperation::Insert { .. } => table.persisted_row_count = table.persisted_row_count.saturating_add(1),
            Pdb6DmlOperation::Delete { .. } => table.persisted_row_count = table.persisted_row_count.saturating_sub(affected_rows),
            Pdb6DmlOperation::Update { .. } => {}
        }
        self.wal_sequence = generation;
        self.revision = self.revision.wrapping_add(1);
        self.ember_indexes_valid = false;

        Ok(QueryResult {
            columns: Vec::new(), rows: Vec::new(), affected_rows,
            message: match statement.get(..6) {
                Some(prefix) if prefix.eq_ignore_ascii_case("INSERT") => "1 row inserted".into(),
                Some(prefix) if prefix.eq_ignore_ascii_case("UPDATE") => format!("{affected_rows} row(s) updated"),
                _ => format!("{affected_rows} row(s) deleted"),
            },
        })
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
                storage_path: self.storage_path.clone(),
                wal_path: None,
                _lock_file: None,
                wal_sequence: self.wal_sequence,
                revision: self.revision,
                pdb6_mode: self.pdb6_mode,
                ember_indexes_valid: false,
                ember_index_journal: Vec::new(),
                ember_journal_supported: true,
            },
            statements: Vec::new(),
        }
    }

    /// Durably prepares a SQL statement batch for the cross-domain coordinator.
    /// The database must already be migrated to PDB6.
    pub fn prepare_sql(
        &mut self,
        transaction_id: u64,
        statements: &[String],
    ) -> Result<(), DbError> {
        if !self.pdb6_mode {
            return Err(DbError::Constraint("SQL coordinator participation requires a PDB6-backed database".into()));
        }
        if statements.is_empty() || statements.len() > 5_000 {
            return Err(DbError::Constraint("prepared SQL batch must contain 1..=5000 statements".into()));
        }
        if !self.pending_prepared_transactions()?.is_empty() {
            return Err(DbError::Storage("SQL has another unresolved prepared transaction".into()));
        }
        let mut session = self.begin_session();
        for statement in statements {
            session.execute(statement)?;
        }
        if session.statements.is_empty() {
            return Err(DbError::Constraint("SQL coordinator participant requires at least one write statement".into()));
        }
        let encoded = encode_coordinator_sql_statements(&session.statements)?;
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
        let mut store = crate::storage::pdb6_store::Pdb6Store::open(path, 128)?;
        store.prepare_participant_batch(transaction_id, &[], &[(b"SQL/STATEMENTS".to_vec(), encoded)])
    }

    pub fn pending_prepared_transactions(&self) -> Result<Vec<u64>, DbError> {
        if !self.pdb6_mode { return Ok(Vec::new()); }
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
        crate::storage::pdb6_store::Pdb6Store::open(path, 128)?.pending_prepared_transactions()
    }

    pub fn commit_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        if !self.pdb6_mode {
            return Err(DbError::Constraint("SQL coordinator participation requires PDB6".into()));
        }
        let path = self.storage_path.clone()
            .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
        let (already_committed, encoded) = {
            let mut store = crate::storage::pdb6_store::Pdb6Store::open(&path, 128)?;
            if store.has_coordinator_commit(transaction_id)? {
                (true, Vec::new())
            } else {
                (false, store.read_prepared_value(transaction_id, b"SQL/STATEMENTS")?
                    .ok_or_else(|| DbError::Storage("prepared SQL statement batch is missing".into()))?)
            }
        };
        if already_committed { return Ok(()); }
        let statements = decode_coordinator_sql_statements(&encoded)?;
        let mut session = self.begin_session();
        for statement in statements { session.execute(&statement)?; }
        self.hydrate_pdb6_changed_tables(&session.staged)?;
        let sequence = self.wal_sequence.checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
        self.wal_sequence = crate::storage::persist_pdb6_transition(
            &path, &self.tables, &session.staged.tables, sequence, Some(transaction_id),
        )?;
        self.tables = session.staged.tables;
        self.revision = self.revision.wrapping_add(1);
        self.ember_indexes_valid = false;
        self.evict_pdb6_row_cache();
        Ok(())
    }

    pub fn finalize_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("SQL participant has no persistent path".into()))?;
        crate::storage::pdb6_store::Pdb6Store::open(path, 128)?.finalize_prepared(transaction_id)
    }

    pub fn rollback_prepared(&mut self, transaction_id: u64) -> Result<(), DbError> {
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("SQL participant has no persistent path".into()))?;
        crate::storage::pdb6_store::Pdb6Store::open(path, 128)?.rollback_prepared(transaction_id)
    }

    fn ensure_no_prepared_sql_transactions(&self) -> Result<(), DbError> {
        if !self.pdb6_mode { return Ok(()); }
        if self.pending_prepared_transactions()?.is_empty() {
            Ok(())
        } else {
            Err(DbError::Storage("SQL writes are blocked while a coordinator transaction is unresolved".into()))
        }
    }

    /// Commit a previously staged session if no other writer changed the database.
    pub fn commit_session(&mut self, session: DatabaseSession) -> Result<(), DbError> {
        if session.base_revision != self.revision {
            return Err(DbError::TransactionConflict);
        }
        if session.statements.is_empty() { return Ok(()); }
        if self.pdb6_mode {
            self.hydrate_pdb6_changed_tables(&session.staged)?;
            self.ensure_no_prepared_sql_transactions()?;
            let path = self.storage_path.as_deref()
                .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
            let sequence = self.wal_sequence.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
            self.wal_sequence = crate::storage::persist_pdb6_transition(
                path, &self.tables, &session.staged.tables, sequence, None,
            )?;
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
        } else {
            self.evict_pdb6_row_cache();
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
            storage_path: self.storage_path.clone(),
            wal_path: None,
            _lock_file: None,
            wal_sequence: self.wal_sequence,
            revision: self.revision,
            pdb6_mode: self.pdb6_mode,
            ember_indexes_valid: false,
            ember_index_journal: Vec::new(),
            ember_journal_supported: true,
        };
        let mut transaction = Transaction {
            database: self,
            staged,
            statements: Vec::new(),
        };
        let value = match operation(&mut transaction) {
            Ok(value) => value,
            Err(error) => {
                drop(transaction);
                self.evict_pdb6_row_cache();
                return Err(error);
            }
        };
        let commit_result = transaction.commit();
        if commit_result.is_err() {
            self.evict_pdb6_row_cache();
        }
        commit_result?;
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
        let rows_loaded = table.rows_loaded;
        if self.pdb6_mode && !rows_loaded {
            return self.insert_many_pdb6(&normalized, batch);
        }
        self.transaction(|transaction| {
            for values in batch {
                let encoded = values.iter().map(value_to_sql).collect::<Vec<_>>().join(", ");
                transaction.execute(&format!("INSERT INTO {normalized} VALUES ({encoded})"))?;
            }
            Ok(inserted)
        })
    }

    /// Bulk-inserts PDB6 rows without hydrating the table cache.
    /// Row payloads, index entries, catalog counters and the manifest share one
    /// bounded CoW publication. Unique keys are checked both against the disk
    /// index and against other rows in this batch.
    fn insert_many_pdb6(&mut self, table_name: &str, batch: Vec<Vec<Value>>) -> Result<usize, DbError> {
        self.ensure_no_prepared_sql_transactions()?;
        let path = self.storage_path.clone()
            .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
        let generation = self.wal_sequence.checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
        let table_schema = self.tables.get(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_owned()))?.clone();
        if table_schema.rows_loaded {
            return Err(DbError::Storage("PDB6 bulk insert expected a cold table cache".into()));
        }

        let mut store = storage::pdb6_store::Pdb6Store::open(&path, 128)?;
        let mut manifest = store.manifest()?
            .ok_or_else(|| DbError::Storage("PDB6 SQL manifest is missing".into()))?;
        let mut catalog = None;
        for table_id in 1..=u64::from(manifest.table_count) {
            let encoded = store.get(&storage::pdb6::table_key(table_id)?)?
                .ok_or_else(|| DbError::Storage(format!("PDB6 manifest references missing table {table_id}")))?;
            let candidate = storage::pdb6::CatalogTable::decode(&encoded)?;
            if candidate.name == table_name {
                catalog = Some(candidate);
                break;
            }
        }
        let mut catalog = catalog.ok_or_else(|| DbError::Storage(format!(
            "PDB6 catalog does not contain table {table_name}"
        )))?;
        let table_id = catalog.table_id;
        if catalog.name != table_name
            || catalog.columns.len() != table_schema.columns.len()
            || catalog.indexes.len() != table_schema.indexes.len()
            || catalog.columns.iter().zip(&table_schema.columns).any(|(stored, memory)| {
                stored.name != memory.name
                    || stored.data_type != memory.data_type
                    || stored.nullable != memory.nullable
            })
        {
            return Err(DbError::Storage("PDB6 catalog and in-memory SQL schema disagree".into()));
        }
        for (stored, (name, index)) in catalog.indexes.iter().zip(table_schema.indexes.iter()) {
            let columns = index.column_indices.iter()
                .map(|column| u32::try_from(*column))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DbError::Storage("PDB6 index column ordinal exceeds u32".into()))?;
            if stored.name != *name || stored.columns != columns || stored.unique != index.unique {
                return Err(DbError::Storage("PDB6 catalog and in-memory SQL index definitions disagree".into()));
            }
        }

        let index_definitions = table_schema.indexes.iter().enumerate()
            .map(|(offset, (name, index))| (name.clone(), (offset as u64) + 1, index.column_indices.clone(), index.unique))
            .collect::<Vec<_>>();
        let mut unique_batch_keys = BTreeMap::<u64, std::collections::BTreeSet<Vec<u8>>>::new();
        let mut row_upserts = Vec::with_capacity(batch.len());
        let mut index_upserts = Vec::with_capacity(batch.len().saturating_mul(index_definitions.len()));
        for values in batch.iter() {
            for (column, value) in table_schema.columns.iter().zip(values) {
                validate_value(column, value)?;
            }
            for (_, index_id, columns, unique) in &index_definitions {
                if !unique { continue; }
                let encoded = storage::index_codec::encode_index_key(
                    &IndexKey::from_columns(values, columns)
                )?;
                if !unique_batch_keys.entry(*index_id).or_default().insert(encoded.clone()) {
                    return Err(DbError::Constraint("bulk INSERT contains duplicate values for a unique SQL index".into()));
                }
                let prefix = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                if !store.scan_prefix(&prefix, 1)?.is_empty() {
                    return Err(DbError::Constraint("bulk INSERT conflicts with an existing unique SQL index value".into()));
                }
            }

            let row_id = catalog.next_row_id;
            catalog.next_row_id = catalog.next_row_id.checked_add(1)
                .ok_or_else(|| DbError::Storage("table row identifier space exhausted".into()))?;
            let payload = storage::sql_rows::SqlRowStore::encode_values(values, table_schema.columns.len())?;
            row_upserts.push((table_id, row_id, payload));
            for (_, index_id, columns, _) in &index_definitions {
                let encoded = storage::index_codec::encode_index_key(
                    &IndexKey::from_columns(values, columns)
                )?;
                let mut key = storage::pdb6::index_key(table_id, *index_id, &encoded)?;
                key.extend_from_slice(&row_id.to_be_bytes());
                if key.len() > 1_024 {
                    return Err(DbError::Constraint("PDB6 index key exceeds 1,024 bytes".into()));
                }
                index_upserts.push((key, Vec::new()));
            }
        }

        let inserted = row_upserts.len();
        if inserted == 0 { return Ok(0); }
        manifest.generation = generation;
        manifest.last_tx_id = manifest.last_tx_id.checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 transaction ID space exhausted".into()))?;
        let metadata_upserts = vec![
            (storage::pdb6::table_key(table_id)?, catalog.encode()?),
            (storage::pdb6::database_root_key(), manifest.encode()?),
        ];
        store.apply_row_batch(row_upserts, metadata_upserts.into_iter().chain(index_upserts), std::iter::empty())?;

        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_owned()))?;
        table.next_row_id = catalog.next_row_id;
        table.persisted_row_count = table.persisted_row_count.checked_add(inserted)
            .ok_or_else(|| DbError::Storage("PDB6 table row count overflow".into()))?;
        self.wal_sequence = generation;
        self.revision = self.revision.wrapping_add(1);
        self.ember_indexes_valid = false;
        Ok(inserted)
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
            persisted_row_count: 0,
            rows_loaded: true,
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
        table.persisted_row_count = table.rows.len();
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
                let expression = parse_where_expression(&table.columns, predicate)?;
                if let Some(equality) = sql_expression_equality_terms(&expression) {
                    table.matching_rows_for_predicates(&equality)
                } else {
                    table.rows.iter().enumerate()
                        .filter(|(_, row)| matches_sql_expression(&row.values, &expression))
                        .map(|(position, _)| position)
                        .collect()
                }
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
                let expression = parse_where_expression(&table.columns, predicate)?;
                let positions = if let Some(equality) = sql_expression_equality_terms(&expression) {
                    table.matching_rows_for_predicates(&equality)
                } else {
                    table.rows.iter().enumerate()
                        .filter(|(_, row)| matches_sql_expression(&row.values, &predicates))
                        .map(|(position, _)| position)
                        .collect()
                };
                positions.into_iter().collect::<std::collections::BTreeSet<_>>()
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
            table.persisted_row_count = table.rows.len();
            table.rebuild_row_positions();
            let removed = before - table.rows.len();
            let deltas = build_rows_index_deltas(&table_name, table, &old_rows, None, false)?;
            (removed, deltas)
        };
        self.append_ember_journal(deltas);
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: removed, message: format!("{removed} row(s) deleted") })
    }

    fn select_pdb6_from_disk(
        &self,
        table_name: &str,
        projection: &str,
        predicate: Option<&str>,
        order_by: Option<&str>,
        limit: Option<usize>,
    ) -> Result<QueryResult, DbError> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.to_owned()))?;
        let projection_upper = projection.to_ascii_uppercase();
        let is_count = projection_upper.starts_with("COUNT(*)");
        let predicates = predicate
            .map(|value| parse_where_expression(&table.columns, value))
            .transpose()?
            .unwrap_or_default();
        let order = order_by.map(|value| parse_order_by(&table.columns, value))
            .transpose()?.unwrap_or_default();
        if is_count && !order.is_empty() {
            return Err(DbError::Unsupported("ORDER BY with COUNT(*) is not supported yet".into()));
        }
        let projection_indices = if is_count {
            Vec::new()
        } else if projection == "*" {
            (0..table.columns.len()).collect::<Vec<_>>()
        } else {
            split_csv(projection).into_iter().map(|name| {
                let normalized = normalize_ident(name);
                table.columns.iter().position(|column| column.name == normalized)
                    .ok_or_else(|| DbError::ColumnNotFound(normalized))
            }).collect::<Result<Vec<_>, _>>()?
        };
        let columns = if is_count {
            vec![projection_upper.find(" AS ")
                .map(|position| normalize_ident(&projection[position + 4..]))
                .unwrap_or_else(|| "count".into())]
        } else {
            projection_indices.iter()
                .map(|index| table.columns[*index].name.clone())
                .collect()
        };
        let path = self.storage_path.as_deref()
            .ok_or_else(|| DbError::Storage("PDB6 database has no storage path".into()))?;
        let mut matched_rows = Vec::new();
        let mut matched = 0usize;
        if limit != Some(0) || is_count {
            storage::scan_pdb6_table_rows(path, table_name, 512, |row| {
                if !matches_sql_expression(&row.values, &predicates) {
                    return Ok(true);
                }
                matched = matched.checked_add(1)
                    .ok_or_else(|| DbError::Storage("SQL result row count overflow".into()))?;
                if !is_count {
                    matched_rows.push(row);
                }
                Ok(is_count || !order.is_empty() || limit.is_none_or(|maximum| matched < maximum))
            })?;
        }
        if is_count {
            matched_rows.push(Row { row_id: 0, values: vec![Value::Integer(matched as i64)] });
        } else {
            if !order.is_empty() {
                matched_rows.sort_by(|left, right| compare_rows_by_order(left, right, &order));
            }
            if let Some(maximum) = limit {
                matched_rows.truncate(maximum);
            }
            for row in &mut matched_rows {
                row.values = projection_indices.iter()
                    .map(|column| row.values[*column].clone())
                    .collect();
            }
        }
        let rows = matched_rows;
        Ok(QueryResult {
            message: format!("{} row(s)", rows.len()),
            rows,
            columns,
            affected_rows: 0,
        })
    }

    fn select(&self, sql: &str) -> Result<QueryResult, DbError> {
        let upper = sql.to_ascii_uppercase();
        let from = upper.find(" FROM ").ok_or_else(|| DbError::Parse("expected FROM".into()))?;
        let projection = sql["SELECT".len()..from].trim();
        let tail = sql[from + 6..].trim();

        let (without_limit, limit) = if let Some(position) = find_unquoted_keyword(tail, " LIMIT ") {
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

        let order_pos = find_unquoted_keyword(without_limit, " ORDER BY ");
        let (query_tail, order_by) = match order_pos {
            Some(position) => (&without_limit[..position], Some(without_limit[position + 10..].trim())),
            None => (without_limit, None),
        };
        let where_pos = find_unquoted_keyword(query_tail, " WHERE ");
        let (table_part, predicate) = match where_pos {
            Some(position) => (&query_tail[..position], Some(query_tail[position + 7..].trim())),
            None => (query_tail, None),
        };
        let table_name = normalize_ident(table_part.split_whitespace().next().unwrap_or(""));
        let table = self.tables.get(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        if self.pdb6_mode {
            return self.select_pdb6_from_disk(&table_name, projection, predicate, order_by, limit);
        }

        let projection_upper = projection.to_ascii_uppercase();
        let is_count = projection_upper.starts_with("COUNT(*)");
        let order = order_by.map(|value| parse_order_by(&table.columns, value))
            .transpose()?.unwrap_or_default();
        if is_count && !order.is_empty() {
            return Err(DbError::Unsupported("ORDER BY with COUNT(*) is not supported yet".into()));
        }
        let selection_limit = if is_count || !order.is_empty() { None } else { limit };
        let selected_rows: Vec<usize> = if let Some(predicate) = predicate {
            let expression = parse_where_expression(&table.columns, predicate)?;
            if let Some(equality_values) = sql_expression_equality_terms(&expression) {
                let columns = equality_values.iter().map(|(index, _)| *index).collect::<Vec<_>>();
                let persistent_index = if self.ember_indexes_valid {
                    self.storage_path.as_deref().and_then(|path| {
                        table.indexes.iter().find(|(_, index)| index.column_indices == columns)
                            .map(|(name, _)| (path, name.as_str()))
                    })
                } else {
                    None
                };
                if let Some((path, index_name)) = persistent_index {
                    let values = equality_values.iter().map(|(_, value)| value.clone()).collect::<Vec<_>>();
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
                        table.matching_rows_for_predicates_limited(&equality_values, selection_limit)
                    }
                } else {
                    table.matching_rows_for_predicates_limited(&equality_values, selection_limit)
                }
            } else {
                table.rows.iter().enumerate()
                    .filter(|(_, row)| matches_sql_expression(&row.values, &predicates))
                    .take(selection_limit.unwrap_or(usize::MAX))
                    .map(|(position, _)| position)
                    .collect()
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
        let mut source_rows = selected_rows.into_iter().filter_map(|row_index| {
            table.rows.get(row_index).cloned()
        }).collect::<Vec<_>>();
        if !order.is_empty() {
            source_rows.sort_by(|left, right| compare_rows_by_order(left, right, &order));
        }
        if let Some(maximum) = limit {
            source_rows.truncate(maximum);
        }
        let rows = source_rows.into_iter().map(|row| Row {
            row_id: row.row_id,
            values: projection_indices.iter().map(|column_index| row.values[*column_index].clone()).collect()
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
const COORDINATOR_SQL_MAGIC: &[u8; 4] = b"PSQL";
fn encode_coordinator_sql_statements(statements: &[String]) -> Result<Vec<u8>, DbError> {
    let mut out = Vec::new();
    out.extend_from_slice(COORDINATOR_SQL_MAGIC);
    out.extend_from_slice(&(statements.len() as u32).to_le_bytes());
    for statement in statements {
        let bytes = statement.as_bytes();
        if bytes.is_empty() || bytes.len() > 16 * 1024 * 1024 {
            return Err(DbError::Constraint("prepared SQL statement must contain 1..=16 MiB".into()));
        }
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
        if out.len() > 256 * 1024 * 1024 {
            return Err(DbError::Constraint("prepared SQL batch exceeds 256 MiB".into()));
        }
    }
    let checksum = crate::protocol::crc32(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    Ok(out)
}

fn decode_coordinator_sql_statements(bytes: &[u8]) -> Result<Vec<String>, DbError> {
    if bytes.len() < 12 || &bytes[..4] != COORDINATOR_SQL_MAGIC
        || u32::from_le_bytes(bytes[bytes.len()-4..].try_into().unwrap()) != crate::protocol::crc32(&bytes[..bytes.len()-4]) {
        return Err(DbError::Storage("prepared SQL statement batch is corrupt".into()));
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if count == 0 || count > 5_000 { return Err(DbError::Storage("prepared SQL statement count is invalid".into())); }
    let end = bytes.len() - 4;
    let mut cursor = 8usize;
    let mut statements = Vec::with_capacity(count);
    for _ in 0..count {
        let length_end = cursor.checked_add(4).ok_or_else(|| DbError::Storage("prepared SQL offset overflow".into()))?;
        if length_end > end { return Err(DbError::Storage("prepared SQL batch is truncated".into())); }
        let length = u32::from_le_bytes(bytes[cursor..length_end].try_into().unwrap()) as usize;
        cursor = length_end;
        let next = cursor.checked_add(length).ok_or_else(|| DbError::Storage("prepared SQL length overflow".into()))?;
        if length == 0 || length > 16 * 1024 * 1024 || next > end {
            return Err(DbError::Storage("prepared SQL statement length is invalid".into()));
        }
        statements.push(String::from_utf8(bytes[cursor..next].to_vec())
            .map_err(|_| DbError::Storage("prepared SQL statement is not UTF-8".into()))?);
        cursor = next;
    }
    if cursor != end { return Err(DbError::Storage("prepared SQL batch has trailing bytes".into())); }
    Ok(statements)
}

fn pdb6_write_table_target(statement: &str) -> Option<String> {
    let identifier = if starts_with_kw(statement, "INSERT INTO") {
        statement["INSERT INTO".len()..].split_whitespace().next()
    } else if starts_with_kw(statement, "UPDATE") {
        statement["UPDATE".len()..].split_whitespace().next()
    } else if starts_with_kw(statement, "DELETE FROM") {
        statement["DELETE FROM".len()..].split_whitespace().next()
    } else if starts_with_kw(statement, "CREATE INDEX") {
        let rest = &statement["CREATE INDEX".len()..];
        let on_position = find_kw(rest, " ON ")?;
        let target = rest[on_position + 4..].trim();
        let end = target.find(|character: char| character == '(' || character.is_whitespace())
            .unwrap_or(target.len());
        Some(&target[..end])
    } else {
        None
    }?;
    let normalized = normalize_ident(identifier);
    (!normalized.is_empty()).then_some(normalized)
}

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
        if is_write {
            if let Some(table_name) = pdb6_write_table_target(statement) {
                self.staged.hydrate_pdb6_table(&table_name)?;
            }
        }
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
        if is_write && self.database.pdb6_mode {
            if let Some(table_name) = pdb6_write_table_target(statement) {
                self.database.hydrate_pdb6_table(&table_name)?;
                self.staged.hydrate_pdb6_table(&table_name)?;
            }
        }
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
            self.database.evict_pdb6_row_cache();
            return Ok(());
        }
        if self.database.pdb6_mode {
            self.database.ensure_no_prepared_sql_transactions()?;
            let path = self.database.storage_path.as_deref()
                .ok_or_else(|| DbError::Storage("PDB6 database has no persistent storage path".into()))?;
            let sequence = self.database.wal_sequence.checked_add(1)
                .ok_or_else(|| DbError::Storage("PDB6 SQL generation space exhausted".into()))?;
            self.database.wal_sequence = crate::storage::persist_pdb6_transition(
                path, &self.database.tables, &self.staged.tables, sequence, None,
            )?;
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
        } else {
            self.database.evict_pdb6_row_cache();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_pdb6_starts_with_disk_backed_sql_and_refuses_overwrite() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-create-pdb6-{}-{}.pbd",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::create_pdb6(&path).unwrap();
            assert!(db.pdb6_mode);
            assert!(Database::create_pdb6(&path).is_err());
            db.execute("CREATE TABLE disk_rows (id INTEGER PRIMARY KEY, value TEXT)").unwrap();
            db.execute("INSERT INTO disk_rows VALUES (1, 'stored')").unwrap();
            assert!(!db.tables["disk_rows"].rows_loaded);
        }
        {
            let mut db = Database::open(&path).unwrap();
            assert!(db.pdb6_mode);
            assert!(!db.tables["disk_rows"].rows_loaded);
            let result = db.execute("SELECT value FROM disk_rows WHERE id = 1").unwrap();
            assert_eq!(result.rows[0].values[0], Value::Text("stored".into()));
            assert!(!db.tables["disk_rows"].rows_loaded);
        }
        for suffix in ["", ".wal", ".lock", ".pdb6.lock", ".pdb6.pages", ".pdb6.wal"] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }

    #[test]
    fn pdb6_single_statement_dml_stages_only_changed_rows_and_keeps_cache_cold() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-pdb6-bounded-dml-{}-{}.pbd",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::create_pdb6(&path).unwrap();
            db.execute("CREATE TABLE players (uuid TEXT PRIMARY KEY, coins INTEGER)").unwrap();
            let rows = (0..2_000).map(|index| vec![
                Value::Text(format!("player-{index:04}")),
                Value::Integer(index),
            ]).collect::<Vec<_>>();
            assert_eq!(db.insert_many("players", rows).unwrap(), 2_000);
            assert!(!db.tables["players"].rows_loaded,
                "PDB6 insert_many must not hydrate the table cache");
            assert_eq!(db.catalog().tables[0].row_count, 2_000);
            assert!(db.insert_many("players", vec![
                vec![Value::Text("player-0000".into()), Value::Integer(9_999)],
                vec![Value::Text("player-3000".into()), Value::Integer(3_000)],
            ]).is_err(), "bulk insert must reject a key already present on disk");
            assert!(db.insert_many("players", vec![
                vec![Value::Text("player-3001".into()), Value::Integer(3_001)],
                vec![Value::Text("player-3001".into()), Value::Integer(3_002)],
            ]).is_err(), "bulk insert must reject duplicate unique keys within one batch");
            assert_eq!(db.catalog().tables[0].row_count, 2_000);
            assert!(!db.tables["players"].rows_loaded);
        }

        {
            let mut db = Database::open(&path).unwrap();
            assert!(!db.tables["players"].rows_loaded);
            assert_eq!(db.catalog().tables[0].row_count, 2_000);

            let updated = db.execute(
                "UPDATE players SET coins = 777 WHERE uuid = 'player-1000'"
            ).unwrap();
            assert_eq!(updated.affected_rows, 1);
            assert!(!db.tables["players"].rows_loaded,
                "bounded UPDATE must not hydrate the full table");

            let unchanged = db.execute(
                "UPDATE players SET coins = 777 WHERE uuid = 'player-1000'"
            ).unwrap();
            assert_eq!(unchanged.affected_rows, 1,
                "same-value UPDATE must preserve matched-row semantics");
            assert!(!db.tables["players"].rows_loaded);

            let key_changed = db.execute(
                "UPDATE players SET uuid = 'player-2000' WHERE uuid = 'player-1000'"
            ).unwrap();
            assert_eq!(key_changed.affected_rows, 1);
            assert_eq!(
                db.execute("SELECT coins FROM players WHERE uuid = 'player-2000'").unwrap().rows[0].values[0],
                Value::Integer(777)
            );
            db.execute("UPDATE players SET uuid = 'player-1000' WHERE uuid = 'player-2000'").unwrap();
            assert!(!db.tables["players"].rows_loaded);

            let duplicate = db.execute(
                "UPDATE players SET uuid = 'player-0002' WHERE uuid = 'player-1001'"
            );
            assert!(matches!(duplicate, Err(DbError::Constraint(_))));
            assert!(!db.tables["players"].rows_loaded);

            let deleted = db.execute(
                "DELETE FROM players WHERE uuid = 'player-1001'"
            ).unwrap();
            assert_eq!(deleted.affected_rows, 1);
            assert!(!db.tables["players"].rows_loaded);
            assert_eq!(db.catalog().tables[0].row_count, 1_999);

            let inserted = db.execute(
                "INSERT INTO players VALUES ('player-1001', 2001)"
            ).unwrap();
            assert_eq!(inserted.affected_rows, 1);
            assert!(!db.tables["players"].rows_loaded);
            assert_eq!(db.catalog().tables[0].row_count, 2_000);

            assert_eq!(
                db.execute("SELECT coins FROM players WHERE uuid = 'player-1000'").unwrap().rows[0].values[0],
                Value::Integer(777)
            );
            assert_eq!(
                db.execute("SELECT coins FROM players WHERE uuid = 'player-1001'").unwrap().rows[0].values[0],
                Value::Integer(2001)
            );
            assert!(!db.tables["players"].rows_loaded);
        }

        {
            let mut db = Database::open(&path).unwrap();
            assert!(!db.tables["players"].rows_loaded);
            assert_eq!(db.catalog().tables[0].row_count, 2_000);
            assert_eq!(
                db.execute("SELECT coins FROM players WHERE uuid = 'player-1000'").unwrap().rows[0].values[0],
                Value::Integer(777)
            );
        }

        for suffix in [
            "", ".wal", ".lock", ".pdb6.lock", ".pdb6.pages", ".pdb6.wal",
            ".pdb6.pages.wal", ".pdb6.pages.lock", ".pdb6.snapshots",
        ] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }

    #[test]
    fn pdb6_writes_hydrate_only_the_tables_they_modify() {
        let path = std::env::temp_dir().join(format!(
            "postemberdb-pdb6-table-hydration-{}-{}.pbd",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        {
            let mut db = Database::create_pdb6(&path).unwrap();
            db.execute("CREATE TABLE players (uuid TEXT, coins INTEGER)").unwrap();
            db.execute("CREATE TABLE regions (region_id INTEGER, payload TEXT)").unwrap();
            db.execute("INSERT INTO players VALUES ('player-a', 10)").unwrap();
            db.execute("INSERT INTO regions VALUES (1, 'region-payload')").unwrap();
        }

        {
            let mut db = Database::open(&path).unwrap();
            assert!(!db.tables["players"].rows_loaded);
            assert!(!db.tables["regions"].rows_loaded);

            db.transaction(|transaction| {
                transaction.execute("UPDATE players SET coins = 20 WHERE uuid = 'player-a'")?;
                assert!(transaction.database.tables["players"].rows_loaded);
                assert!(!transaction.database.tables["regions"].rows_loaded);
                assert!(transaction.staged.tables["players"].rows_loaded);
                assert!(!transaction.staged.tables["regions"].rows_loaded);
                Ok(())
            }).unwrap();

            assert!(!db.tables["players"].rows_loaded, "committed row caches must be evicted");
            assert!(!db.tables["regions"].rows_loaded, "unmodified tables must stay cold");

            let mut session = db.begin_session();
            session.execute("UPDATE players SET coins = 30 WHERE uuid = 'player-a'").unwrap();
            assert!(session.staged.tables["players"].rows_loaded);
            assert!(!session.staged.tables["regions"].rows_loaded);
            db.commit_session(session).unwrap();

            assert!(!db.tables["players"].rows_loaded);
            assert!(!db.tables["regions"].rows_loaded);
            assert_eq!(
                db.execute("SELECT coins FROM players WHERE uuid = 'player-a'").unwrap().rows[0].values[0],
                Value::Integer(30)
            );
            assert_eq!(
                db.execute("SELECT payload FROM regions WHERE region_id = 1").unwrap().rows[0].values[0],
                Value::Text("region-payload".into())
            );
            assert!(!db.tables["players"].rows_loaded);
            assert!(!db.tables["regions"].rows_loaded);
        }

        for suffix in [
            "", ".wal", ".lock", ".pdb6.lock", ".pdb6.pages", ".pdb6.wal",
            ".pdb6.pages.wal", ".pdb6.pages.lock",
        ] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }

    #[test]
    fn pdb6_write_target_parser_handles_dml_and_index_statements() {
        assert_eq!(pdb6_write_table_target("INSERT INTO players VALUES (1)"), Some("players".into()));
        assert_eq!(pdb6_write_table_target("UPDATE `Players` SET coins = 1"), Some("players".into()));
        assert_eq!(pdb6_write_table_target("DELETE FROM regions WHERE region_id = 1"), Some("regions".into()));
        assert_eq!(pdb6_write_table_target("CREATE INDEX idx_uuid ON players (uuid)"), Some("players".into()));
        assert_eq!(pdb6_write_table_target("CREATE TABLE players (uuid TEXT)"), None);
        assert_eq!(pdb6_write_table_target("CREATE INDEX idx_uuid players (uuid)"), None);
    }

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
            assert!(!db.tables["players"].rows_loaded, "PDB6 open must not hydrate row payloads");
            assert!(db.tables["players"].rows.is_empty());
            assert_eq!(db.catalog().tables[0].row_count, 2);
            assert_eq!(db.execute("SELECT * FROM players WHERE uuid = 'player-a'").unwrap().rows.len(), 1);
            assert!(!db.tables["players"].rows_loaded, "SELECT must keep the table row cache cold");
            let ordered = db.execute("SELECT uuid FROM players ORDER BY coins DESC").unwrap();
            assert_eq!(ordered.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
                vec![Value::Text("player-b".into()), Value::Text("player-a".into())]);
            let ranged = db.execute("SELECT uuid FROM players WHERE coins >= 20").unwrap();
            assert_eq!(ranged.rows[0].values[0], Value::Text("player-b".into()));
            assert!(!db.tables["players"].rows_loaded, "filtered SELECT must not hydrate the table cache");
            assert!(!db.tables["players"].rows_loaded, "ordered SELECT must not hydrate the table cache");
            db.execute("UPDATE players SET coins = 99 WHERE uuid = 'player-a'").unwrap();
            assert!(!db.tables["players"].rows_loaded, "committed DML must evict its temporary full-table staging cache");
            db.execute("DELETE FROM players WHERE uuid = 'player-b'").unwrap();
            db.execute("INSERT INTO players VALUES ('player-c', 30)").unwrap();
        }
        {
            let mut db = Database::open(&path).unwrap();
            assert!(!db.tables["players"].rows_loaded);
            let rows = db.execute("SELECT * FROM players").unwrap().rows;
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().any(|row| row.values == vec![Value::Text("player-a".into()), Value::Integer(99)]));
            assert!(rows.iter().any(|row| row.values == vec![Value::Text("player-c".into()), Value::Integer(30)]));
            assert!(db.execute("SELECT * FROM players WHERE uuid = 'player-b'").unwrap().rows.is_empty());

            // Prove that SQL SELECT and table paging fetch committed payloads
            // from PDB6 pages rather than depending on the reconstructed RAM cache.
            let table = db.tables.get_mut("players").unwrap();
            table.rows.clear();
            table.row_positions.clear();
            assert_eq!(db.read_table_page("players", 0, 1).unwrap().rows[0].values,
                vec![Value::Text("player-a".into()), Value::Integer(99)]);
            let selected = db.execute("SELECT coins FROM players WHERE uuid = 'player-c' LIMIT 1").unwrap();
            assert_eq!(selected.rows[0].values, vec![Value::Integer(30)]);
            let count = db.execute("SELECT COUNT(*) FROM players").unwrap();
            assert_eq!(count.rows[0].values, vec![Value::Integer(2)]);
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
    fn catalog_exposes_table_columns_indexes_and_row_counts() {
        let mut db = Database::new();
        db.execute("CREATE TABLE browser_players (uuid TEXT PRIMARY KEY, coins INTEGER NOT NULL)").unwrap();
        db.execute("CREATE INDEX browser_players_coins ON browser_players (coins)").unwrap();
        db.execute("INSERT INTO browser_players VALUES ('player-a', 42)").unwrap();

        let catalog = db.catalog();
        assert_eq!(catalog.tables.len(), 1);
        let table = &catalog.tables[0];
        assert_eq!(table.name, "browser_players");
        assert_eq!(table.row_count, 1);
        assert_eq!(table.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            vec!["uuid", "coins"]);
        assert!(!table.columns[0].nullable);
        assert!(!table.columns[1].nullable);
        let primary = table.indexes.iter().find(|index| index.name == "__pk_browser_players").unwrap();
        assert!(primary.unique);
        assert_eq!(primary.columns, vec!["uuid"]);
        let secondary = table.indexes.iter().find(|index| index.name == "browser_players_coins").unwrap();
        assert!(!secondary.unique);
        assert_eq!(secondary.columns, vec!["coins"]);

        let detail = db.table_info("BROWSER_PLAYERS").unwrap();
        assert_eq!(detail.row_count, 1);
        assert!(db.table_info("missing_table").is_err());
    }

    #[test]
    fn table_pages_are_bounded_and_preserve_stable_row_ids() {
        let mut db = Database::new();
        db.execute("CREATE TABLE page_test (id INTEGER PRIMARY KEY, label TEXT)").unwrap();
        for id in 1..=5 {
            db.execute(&format!("INSERT INTO page_test VALUES ({id}, 'row-{id}')")).unwrap();
        }

        let first = db.read_table_page("page_test", 0, 2).unwrap();
        let second = db.read_table_page("page_test", 2, 2).unwrap();
        let beyond_end = db.read_table_page("page_test", 20, 2).unwrap();

        assert_eq!(first.rows.iter().map(|row| row.row_id).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(second.rows.iter().map(|row| row.row_id).collect::<Vec<_>>(), vec![3, 4]);
        assert_eq!(second.rows[0].values[1], Value::Text("row-3".into()));
        assert!(beyond_end.rows.is_empty());
        assert!(db.read_table_page("page_test", 0, 1_001).is_err());
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
    fn where_supports_comparisons_null_checks_and_like_for_select_update_delete() {
        let mut db = Database::new();
        db.execute("CREATE TABLE predicate_rows (name TEXT, score INTEGER, note TEXT)").unwrap();
        db.execute("INSERT INTO predicate_rows VALUES ('Ada', 30, NULL)").unwrap();
        db.execute("INSERT INTO predicate_rows VALUES ('Bob', 20, 'active')").unwrap();
        db.execute("INSERT INTO predicate_rows VALUES ('Alice', 10, 'idle')").unwrap();

        let selected = db.execute("SELECT name FROM predicate_rows WHERE score >= 20 AND name LIKE 'A%'").unwrap();
        assert_eq!(selected.rows.len(), 1);
        assert_eq!(selected.rows[0].values[0], Value::Text("Ada".into()));

        let nulls = db.execute("SELECT name FROM predicate_rows WHERE note IS NULL").unwrap();
        assert_eq!(nulls.rows[0].values[0], Value::Text("Ada".into()));
        assert!(db.execute("SELECT name FROM predicate_rows WHERE score = NULL").unwrap().rows.is_empty());

        let not_equal = db.execute("SELECT name FROM predicate_rows WHERE score <> 20 ORDER BY score DESC").unwrap();
        assert_eq!(not_equal.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Text("Ada".into()), Value::Text("Alice".into())]);

        db.execute("UPDATE predicate_rows SET score = 40 WHERE score > 25").unwrap();
        assert_eq!(db.execute("SELECT score FROM predicate_rows WHERE name = 'Ada'").unwrap().rows[0].values[0], Value::Integer(40));
        db.execute("DELETE FROM predicate_rows WHERE note IS NOT NULL AND score <= 20").unwrap();
        assert_eq!(db.execute("SELECT COUNT(*) FROM predicate_rows").unwrap().rows[0].values[0], Value::Integer(1));
    }

    #[test]
    fn select_supports_order_by_ascending_descending_and_multiple_keys() {
        let mut db = Database::new();
        db.execute("CREATE TABLE ordered_rows (category TEXT, score INTEGER)").unwrap();
        db.execute("INSERT INTO ordered_rows VALUES ('b', 10)").unwrap();
        db.execute("INSERT INTO ordered_rows VALUES ('a', 10)").unwrap();
        db.execute("INSERT INTO ordered_rows VALUES ('c', 30)").unwrap();
        let descending = db.execute("SELECT category, score FROM ordered_rows ORDER BY score DESC, category ASC LIMIT 2").unwrap();
        assert_eq!(descending.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Text("c".into()), Value::Text("a".into())]);
        let ascending = db.execute("SELECT category FROM ordered_rows ORDER BY score ASC, category DESC").unwrap();
        assert_eq!(ascending.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Text("b".into()), Value::Text("a".into()), Value::Text("c".into())]);
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
    fn row_staging_memory_estimate_accounts_for_owned_text_capacity() {
        let compact = vec![Value::Integer(7), Value::Boolean(true)];
        let text = "a".repeat(4_096);
        let with_text = vec![Value::Integer(7), Value::Text(text)];

        assert!(
            estimate_row_values_heap_bytes(&with_text)
                >= estimate_row_values_heap_bytes(&compact) + 4_096,
            "the staging budget must account for heap-owned TEXT payloads"
        );
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

    #[test]
    fn where_or_groups_follow_sql_and_before_or_precedence() {
        let mut db = Database::new();
        db.execute("CREATE TABLE logical_filters (id INTEGER, state TEXT)").unwrap();
        db.execute("INSERT INTO logical_filters VALUES (1, 'cold')").unwrap();
        db.execute("INSERT INTO logical_filters VALUES (2, 'ready')").unwrap();
        db.execute("INSERT INTO logical_filters VALUES (3, 'cold')").unwrap();
        db.execute("INSERT INTO logical_filters VALUES (4, 'ready')").unwrap();
        db.execute("CREATE INDEX idx_logical_filters_id ON logical_filters (id)").unwrap();

        let either = db.execute(
            "SELECT id FROM logical_filters WHERE id = 1 OR id = 3 ORDER BY id"
        ).unwrap();
        assert_eq!(
            either.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(3)]
        );

        // AND has higher precedence: id=1 OR (id=2 AND state='ready').
        let precedence = db.execute(
            "SELECT id FROM logical_filters WHERE id = 1 OR id = 2 AND state = 'ready' ORDER BY id"
        ).unwrap();
        assert_eq!(
            precedence.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(2)]
        );

        // OR inside a quoted string is data, not a logical separator.
        db.execute("INSERT INTO logical_filters VALUES (5, 'left OR right')").unwrap();
        let quoted = db.execute(
            "SELECT id FROM logical_filters WHERE state = 'left OR right' OR id = 3 ORDER BY id"
        ).unwrap();
        assert_eq!(
            quoted.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(3), Value::Integer(5)]
        );

        let updated = db.execute(
            "UPDATE logical_filters SET state = 'changed' WHERE id = 1 OR id = 3"
        ).unwrap();
        assert_eq!(updated.affected_rows, 2);
        let changed = db.execute(
            "SELECT id FROM logical_filters WHERE state = 'changed' OR id = 5 ORDER BY id"
        ).unwrap();
        assert_eq!(changed.rows.len(), 3);

        let deleted = db.execute(
            "DELETE FROM logical_filters WHERE id = 2 OR id = 4"
        ).unwrap();
        assert_eq!(deleted.affected_rows, 2);
        assert_eq!(db.execute("SELECT * FROM logical_filters").unwrap().rows.len(), 3);
    }


    #[test]
    fn where_in_not_in_and_between_predicates_follow_sql_null_rules() {
        let mut db = Database::new();
        db.execute("CREATE TABLE sql_ranges (id INTEGER, label TEXT)").unwrap();
        for id in 1..=6 {
            db.execute(&format!("INSERT INTO sql_ranges VALUES ({id}, 'item-{id}')")).unwrap();
        }
        db.execute("INSERT INTO sql_ranges VALUES (NULL, 'unknown')").unwrap();

        let not_like_rows = db.execute(
            "SELECT id FROM sql_ranges WHERE label NOT LIKE 'item-%'"
        ).unwrap();
        assert_eq!(not_like_rows.rows.len(), 1);
        assert_eq!(not_like_rows.rows[0].values[0], Value::Null);

        let in_rows = db.execute(
            "SELECT id FROM sql_ranges WHERE id IN (1, 3, 5) ORDER BY id"
        ).unwrap();
        assert_eq!(
            in_rows.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(3), Value::Integer(5)]
        );

        let not_in_rows = db.execute(
            "SELECT id FROM sql_ranges WHERE id NOT IN (1, 3, 5) ORDER BY id"
        ).unwrap();
        assert_eq!(
            not_in_rows.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(2), Value::Integer(4), Value::Integer(6)]
        );

        let between_rows = db.execute(
            "SELECT id FROM sql_ranges WHERE id BETWEEN 2 AND 4 ORDER BY id"
        ).unwrap();
        assert_eq!(
            between_rows.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(2), Value::Integer(3), Value::Integer(4)]
        );

        let not_between_rows = db.execute(
            "SELECT id FROM sql_ranges WHERE id NOT BETWEEN 2 AND 4 ORDER BY id"
        ).unwrap();
        assert_eq!(
            not_between_rows.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(5), Value::Integer(6)]
        );

        let between_and = db.execute(
            "SELECT id FROM sql_ranges WHERE id BETWEEN 2 AND 5 AND label = 'item-3'"
        ).unwrap();
        assert_eq!(between_and.rows.len(), 1);
        assert_eq!(between_and.rows[0].values[0], Value::Integer(3));

        let between_or = db.execute(
            "SELECT id FROM sql_ranges WHERE id = 1 OR id BETWEEN 3 AND 4 ORDER BY id"
        ).unwrap();
        assert_eq!(
            between_or.rows.iter().map(|row| row.values[0].clone()).collect::<Vec<_>>(),
            vec![Value::Integer(1), Value::Integer(3), Value::Integer(4)]
        );

        let distinct_from_value = db.execute(
            "SELECT id FROM sql_ranges WHERE id IS DISTINCT FROM 3 ORDER BY id"
        ).unwrap();
        assert_eq!(distinct_from_value.rows.len(), 6);
        assert_eq!(distinct_from_value.rows.first().unwrap().values[0], Value::Null);

        let not_distinct_from_null = db.execute(
            "SELECT id FROM sql_ranges WHERE id IS NOT DISTINCT FROM NULL"
        ).unwrap();
        assert_eq!(not_distinct_from_null.rows.len(), 1);
        assert_eq!(not_distinct_from_null.rows[0].values[0], Value::Null);

        let distinct_from_null = db.execute(
            "SELECT id FROM sql_ranges WHERE id IS DISTINCT FROM NULL"
        ).unwrap();
        assert_eq!(distinct_from_null.rows.len(), 6);
        assert!(distinct_from_null.rows.iter().all(|row| !matches!(&row.values[0], Value::Null)));

        let not_distinct_from_value = db.execute(
            "SELECT id FROM sql_ranges WHERE id IS NOT DISTINCT FROM 3"
        ).unwrap();
        assert_eq!(not_distinct_from_value.rows.len(), 1);
        assert_eq!(not_distinct_from_value.rows[0].values[0], Value::Integer(3));

        assert!(matches!(
            db.execute("SELECT id FROM sql_ranges WHERE id IS DISTINCT FROM 'three'"),
            Err(DbError::TypeMismatch { .. })
        ));

        let in_with_null = db.execute(
            "SELECT id FROM sql_ranges WHERE id IN (3, NULL)"
        ).unwrap();
        assert_eq!(in_with_null.rows.len(), 1);
        let not_in_with_null = db.execute(
            "SELECT id FROM sql_ranges WHERE id NOT IN (3, NULL)"
        ).unwrap();
        assert!(not_in_with_null.rows.is_empty());

        let updated = db.execute(
            "UPDATE sql_ranges SET label = 'chosen' WHERE id IN (2, 4)"
        ).unwrap();
        assert_eq!(updated.affected_rows, 2);
        let chosen = db.execute(
            "SELECT id FROM sql_ranges WHERE label = 'chosen' ORDER BY id"
        ).unwrap();
        assert_eq!(chosen.rows.len(), 2);

        let deleted = db.execute(
            "DELETE FROM sql_ranges WHERE id NOT BETWEEN 1 AND 4"
        ).unwrap();
        assert_eq!(deleted.affected_rows, 2);
        assert_eq!(db.execute("SELECT * FROM sql_ranges").unwrap().rows.len(), 5);

        assert!(db.execute("SELECT * FROM sql_ranges WHERE id IN ()").is_err());
        assert!(db.execute("SELECT * FROM sql_ranges WHERE id BETWEEN 1 AND").is_err());
    }


}
