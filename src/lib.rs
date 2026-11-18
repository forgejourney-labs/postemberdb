//! PostemberDB: an independent, Rust-native SQL-like database engine.
//!
//! The first implementation is intentionally in-memory. Persistence, transactions,
//! indexing, and the network protocol will be developed as separate engine layers.

use std::collections::BTreeMap;
use std::fmt;

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
        }
    }
}

impl std::error::Error for DbError {}

#[derive(Debug, Clone)]
struct Table {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

#[derive(Debug, Default)]
pub struct Database {
    tables: BTreeMap<String, Table>,
}

impl Database {
    pub fn new() -> Self { Self::default() }

    /// Execute one SQL statement. Supported initially: CREATE TABLE, INSERT, SELECT *.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult, DbError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        if starts_with_kw(statement, "CREATE TABLE") {
            self.create_table(statement)
        } else if starts_with_kw(statement, "INSERT INTO") {
            self.insert(statement)
        } else if starts_with_kw(statement, "SELECT") {
            self.select(statement)
        } else {
            Err(DbError::Unsupported(statement.to_owned()))
        }
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
        self.tables.insert(name.clone(), Table { columns, rows: Vec::new() });
        Ok(QueryResult::message(format!("table {name} created")))
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
        Ok(QueryResult { columns: vec![], rows: vec![], affected_rows: 1, message: "1 row inserted".into() })
    }

    fn select(&self, sql: &str) -> Result<QueryResult, DbError> {
        let upper = sql.to_ascii_uppercase();
        let from = upper.find(" FROM ").ok_or_else(|| DbError::Parse("expected FROM".into()))?;
        let projection = sql["SELECT".len()..from].trim();
        if projection != "*" { return Err(DbError::Unsupported("only SELECT * is supported in this initial version".into())); }
        let tail = sql[from + 6..].trim();
        let table_name = normalize_ident(tail.split_whitespace().next().unwrap_or(""));
        let table = self.tables.get(&table_name).ok_or_else(|| DbError::TableNotFound(table_name.clone()))?;
        Ok(QueryResult {
            columns: table.columns.iter().map(|c| c.name.clone()).collect(),
            rows: table.rows.clone(),
            affected_rows: 0,
            message: format!("{} row(s)", table.rows.len()),
        })
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
    fn rejects_wrong_types() {
        let mut db = Database::new();
        db.execute("CREATE TABLE metrics (value INTEGER)").unwrap();
        assert!(db.execute("INSERT INTO metrics VALUES ('wrong')").is_err());
    }
}
