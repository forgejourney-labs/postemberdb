//! PostemberDB: an independent, Rust-native SQL-like database engine.
//!
//! The engine supports both in-memory databases and an initial persistent file format.
//! Transactions, indexing, and crash recovery remain future engine layers.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

mod storage;

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
        }
    }
}

impl std::error::Error for DbError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKey {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl From<&Value> for IndexKey {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(value) => Self::Integer(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Boolean(value) => Self::Boolean(*value),
        }
    }
}

#[derive(Debug, Clone)]
struct Index {
    column_index: usize,
    entries: BTreeMap<IndexKey, Vec<usize>>,
}

#[derive(Debug, Clone)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>,
    indexes: BTreeMap<String, Index>,
}

impl Table {
    fn add_index(&mut self, index_name: String, column_name: &str) -> Result<(), DbError> {
        if self.indexes.contains_key(&index_name) {
            return Err(DbError::Constraint(format!("index already exists: {index_name}")));
        }
        let column_index = self.columns.iter().position(|column| column.name == normalize_ident(column_name))
            .ok_or_else(|| DbError::ColumnNotFound(column_name.to_owned()))?;
        let mut index = Index { column_index, entries: BTreeMap::new() };
        for (row_index, row) in self.rows.iter().enumerate() {
            index.entries.entry(IndexKey::from(&row.values[column_index])).or_default().push(row_index);
        }
        self.indexes.insert(index_name, index);
        Ok(())
    }

    fn index_row(&mut self, row_index: usize) {
        let row = &self.rows[row_index];
        for index in self.indexes.values_mut() {
            index.entries.entry(IndexKey::from(&row.values[index.column_index]))
                .or_default().push(row_index);
        }
    }
}

#[derive(Debug, Default)]
pub struct Database {
    tables: BTreeMap<String, Table>,
    storage_path: Option<PathBuf>,
    wal_path: Option<PathBuf>,
    wal_sequence: u64,
}

impl Database {
    pub fn new() -> Self { Self::default() }

    /// Execute one SQL statement. Supported initially: CREATE TABLE, INSERT, SELECT *.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        let is_write = starts_with_kw(statement, "CREATE TABLE") || starts_with_kw(statement, "CREATE INDEX") || starts_with_kw(statement, "INSERT INTO");
        if is_write && self.storage_path.is_some() {
            return self.transaction(|transaction| transaction.execute(statement));
        }
        self.execute_inner(statement)
    }

    fn execute_inner(&mut self, statement: &str) -> Result<QueryResult, DbError> {
        let result = if starts_with_kw(statement, "CREATE TABLE") {
            self.create_table(statement)
        } else if starts_with_kw(statement, "CREATE INDEX") {
            self.create_index(statement)
        } else if starts_with_kw(statement, "INSERT INTO") {
            self.insert(statement)
        } else if starts_with_kw(statement, "SELECT") {
            self.select(statement)
        } else {
            Err(DbError::Unsupported(statement.to_owned()))
        };
        result
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
            wal_sequence: self.wal_sequence,
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
        let mut batch = rows.into_iter().take(MAX_BATCH_ROWS + 1).collect::<Vec<_>>();
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

        if self.storage_path.is_some() {
            let inserted = batch.len();
            return self.transaction(|transaction| {
                for values in batch {
                    let encoded = values.iter().map(value_to_sql).collect::<Vec<_>>().join(", ");
                    transaction.execute(&format!("INSERT INTO {normalized} VALUES ({encoded})"))?;
                }
                Ok(inserted)
            });
        }

        let table = self.tables.get_mut(&normalized).expect("table checked above");
        let previous_len = table.rows.len();
        table.rows.extend(batch.drain(..).map(|values| Row { values }));
        let inserted = table.rows.len() - previous_len;

        if self.storage_path.is_some() {
            if let Err(error) = self.save() {
                if let Some(table) = self.tables.get_mut(&normalized) {
                    table.rows.truncate(previous_len);
                }
                return Err(error);
            }
        }
        Ok(inserted)
    }

    fn create_table(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["CREATE TABLE".len()..].trim();
        let open = rest.find('(').ok_or_else(|| DbError::Parse("expected '(' after table name".into()))?;
        let close = rest.rfind(')').ok_or_else(|| DbError::Parse("expected ')'".into()))?;
        let name = normalize_ident(rest[..open].trim());
        if name.is_empty() { return Err(DbError::Parse("missing table name".into())); }
        if self.tables.contains_key(&name) { return Err(DbError::TableExists(name)); }

        let mut columns = Vec::new();
        for definition in split_csv(&rest[open + 1..close]) {
            let mut parts = definition.split_whitespace();
            let column_name = parts.next().ok_or_else(|| DbError::Parse("empty column definition".into()))?;
            let type_name = parts.next().ok_or_else(|| DbError::Parse(format!("missing type for {column_name}")))?;
            let data_type = match type_name.to_ascii_uppercase().as_str() {
                "INT" | "INTEGER" | "BIGINT" => DataType::Integer,
                "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                other => return Err(DbError::Unsupported(format!("column type {other}"))),
            };
            let nullable = !definition.to_ascii_uppercase().contains("NOT NULL");
            columns.push(Column { name: normalize_ident(column_name), data_type, nullable });
        }
        if columns.is_empty() { return Err(DbError::Parse("a table needs at least one column".into())); }
        self.tables.insert(name.clone(), Table { columns, rows: Vec::new(), indexes: BTreeMap::new() });
        Ok(QueryResult::message(format!("table {name} created")))
    }

    fn create_index(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["CREATE INDEX".len()..].trim();
        let on_pos = find_kw(rest, "ON").ok_or_else(|| DbError::Parse("expected ON in CREATE INDEX".into()))?;
        let index_name = normalize_ident(rest[..on_pos].trim());
        let target = rest[on_pos + 2..].trim();
        let open = target.find('(').ok_or_else(|| DbError::Parse("expected '(' with indexed column".into()))?;
        let close = target.rfind(')').ok_or_else(|| DbError::Parse("expected ')' after indexed column".into()))?;
        let table_name = normalize_ident(target[..open].trim());
        let column_name = normalize_ident(target[open + 1..close].trim());
        if index_name.is_empty() || column_name.is_empty() {
            return Err(DbError::Parse("index and column names are required".into()));
        }
        let table = self.tables.get_mut(&table_name)
            .ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        table.add_index(index_name.clone(), &column_name)?;
        Ok(QueryResult::message(format!("index {index_name} created on {table_name}({column_name})")))
    }

    fn insert(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let rest = sql["INSERT INTO".len()..].trim();
        let values_pos = find_kw(rest, "VALUES").ok_or_else(|| DbError::Parse("expected VALUES".into()))?;
        let table_name = normalize_ident(rest[..values_pos].trim().trim_end_matches(')').split('(').next().unwrap_or("").trim());
        let values_part = rest[values_pos + "VALUES".len()..].trim();
        let values_part = values_part.strip_prefix('(').and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| DbError::Parse("expected VALUES (...)".into()))?;
        let values = split_csv(values_part).into_iter().map(parse_value).collect::<Result<Vec<_>, _>>()?;
        let table = self.tables.get_mut(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        if values.len() != table.columns.len() {
            return Err(DbError::Constraint(format!("expected {} values, received {}", table.columns.len(), values.len())));
        }
        for (column, value) in table.columns.iter().zip(values.iter()) {
            validate_value(column, value)?;
        }
        table.rows.push(Row { values });
        let row_index = table.rows.len() - 1;
        table.index_row(row_index);
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row inserted".into() })
    }

    fn select(&self, sql: &str) -> Result<QueryResult, DbError> {
        let upper = sql.to_ascii_uppercase();
        let from = upper.find(" FROM ").ok_or_else(|| DbError::Parse("expected FROM".into()))?;
        let projection = sql["SELECT".len()..from].trim();
        if projection != "*" { return Err(DbError::Unsupported("only SELECT * is supported".into())); }
        let tail = sql[from + 6..].trim();
        let upper_tail = tail.to_ascii_uppercase();
        let where_pos = upper_tail.find(" WHERE ");
        let (table_part, predicate) = match where_pos {
            Some(position) => (&tail[..position], Some(tail[position + 7..].trim())),
            None => (tail, None),
        };
        let table_name = normalize_ident(table_part.split_whitespace().next().unwrap_or(""));
        let table = self.tables.get(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        let selected_rows: Vec<usize> = if let Some(predicate) = predicate {
            let equals = predicate.find('=').ok_or_else(|| DbError::Parse("expected '=' in WHERE predicate".into()))?;
            let column_name = normalize_ident(predicate[..equals].trim());
            let value = parse_value(predicate[equals + 1..].trim())?;
            let column_index = table.columns.iter().position(|column| column.name == column_name)
                .ok_or_else(|| DbError::ColumnNotFound(column_name.clone()))?;
            if let Some(index) = table.indexes.values().find(|index| index.column_index == column_index) {
                index.entries.get(&IndexKey::from(&value)).cloned().unwrap_or_default()
            } else {
                table.rows.iter().enumerate()
                    .filter_map(|(row_index, row)| (row.values[column_index] == value).then_some(row_index))
                    .collect()
            }
        } else {
            (0..table.rows.len()).collect()
        };
        let rows = selected_rows.into_iter().filter_map(|index| table.rows.get(index).cloned()).collect::<Vec<_>>();
        Ok(QueryResult {
            columns: table.columns.iter().map(|column| column.name.clone()).collect(),
            message: format!("{} row(s)", rows.len()),
            rows,
            affected_rows: 0,
        })
    }
}


fn value_to_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Boolean(value) => if *value { "TRUE".into() } else { "FALSE".into() },
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
    }
}

fn starts_with_kw(input: &str, keyword: &str) -> bool {
    input.get(..keyword.len()).map(|s| s.eq_ignore_ascii_case(keyword)).unwrap_or(false)
}

fn find_kw(input: &str, keyword: &str) -> Option<usize> {
    input.to_ascii_uppercase().find(&keyword.to_ascii_uppercase())
}

fn normalize_ident(input: &str) -> String {
    input.trim().trim_matches('`').trim_matches('"').to_ascii_lowercase()
}

fn split_csv(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let (mut start, mut quoted, mut quote_char) = (0, false, '\0');
    for (i, ch) in input.char_indices() {
        if (ch == '\'' || ch == '"') && (!quoted || ch == quote_char) {
            if quoted { quoted = false; } else { quoted = true; quote_char = ch; }
        } else if ch == ',' && !quoted {
            parts.push(input[start..i].trim());
            start = i + 1;
        }
    }
    parts.push(input[start..].trim());
    parts
}

fn parse_value(input: &str) -> Result<Value, DbError> {
    let s = input.trim();
    if s.eq_ignore_ascii_case("NULL") { return Ok(Value::Null); }
    if s.eq_ignore_ascii_case("TRUE") { return Ok(Value::Boolean(true)); }
    if s.eq_ignore_ascii_case("FALSE") { return Ok(Value::Boolean(false)); }
    if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        return Ok(Value::Text(s[1..s.len()-1].replace("''", "'")));
    }
    if let Ok(v) = s.parse::<i64>() { return Ok(Value::Integer(v)); }
    Err(DbError::Parse(format!("invalid value: {s}")))
}

fn validate_value(column: &Column, value: &Value) -> Result<(), DbError> {
    if matches!(value, Value::Null) {
        if !column.nullable { return Err(DbError::Constraint(format!("{} may not be NULL", column.name))); }
        return Ok(());
    }
    let valid = matches!((&column.data_type, value),
        (DataType::Integer, Value::Integer(_)) |
        (DataType::Text, Value::Text(_)) |
        (DataType::Boolean, Value::Boolean(_)));
    if valid { Ok(()) } else {
        Err(DbError::TypeMismatch { column: column.name.clone(), expected: column.data_type.clone() })
    }
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
        let is_write = starts_with_kw(statement, "CREATE TABLE") || starts_with_kw(statement, "CREATE INDEX") || starts_with_kw(statement, "INSERT INTO");
        let result = self.staged.execute_inner(statement)?;
        if is_write {
            self.statements.push(statement.to_owned());
        }
        Ok(result)
    }

    /// Commit all staged writes as one WAL transaction.
    pub fn commit(self) -> Result<(), DbError> {
        if !self.statements.is_empty() && self.database.wal_path.is_some() {
            let sequence = self.database.append_wal_transaction(&self.statements)?;
            self.database.wal_sequence = sequence;
        }
        self.database.tables = self.staged.tables;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn rejects_wrong_types() {
        let mut db = Database::new();
        db.execute("CREATE TABLE metrics (value INTEGER)").unwrap();
        assert!(db.execute("INSERT INTO metrics VALUES ('wrong')").is_err());
    }
}
