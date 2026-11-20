use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use postemberdb::{
    CatalogSnapshot, DataType, Database, QueryResult, RemoteClient, RemoteServerStatus,
    TableInfo, Value,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{net::SocketAddr, sync::{Arc, Mutex}};

const INDEX_HTML: &str = include_str!("../static/index.html");
const MAX_PAGE_SIZE: usize = 200;
const MAX_SQL_BYTES: usize = 64 * 1024;

#[derive(Clone)]
struct AppState {
    database: Arc<Mutex<Database>>,
    remote: Arc<Mutex<Option<RemoteClient>>>,
}

#[derive(Deserialize)]
struct QueryRequest { sql: String }
#[derive(Deserialize)]
struct PageParams { offset: Option<usize>, limit: Option<usize> }
#[derive(Deserialize)]
struct InsertRowRequest { values: Vec<JsonValue> }
#[derive(Deserialize)]
struct UpdateCellRequest { key: Vec<JsonValue>, column: String, value: JsonValue }
#[derive(Deserialize)]
struct DeleteRowRequest { key: Vec<JsonValue> }
#[derive(Deserialize)]
struct RemoteConnectRequest {
    address: String,
    server_name: String,
    ca_bundle_path: String,
    username: String,
    password: String,
}
#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    affected_rows: usize,
    message: String,
}
#[derive(Serialize)]
struct CatalogResponse { tables: Vec<TableResponse> }
#[derive(Serialize)]
struct TableResponse {
    name: String,
    row_count: usize,
    columns: Vec<ColumnResponse>,
    indexes: Vec<IndexResponse>,
}
#[derive(Serialize)]
struct ColumnResponse { name: String, data_type: String, nullable: bool }
#[derive(Serialize)]
struct IndexResponse { name: String, columns: Vec<String>, unique: bool }
#[derive(Serialize)]
struct TableRowsResponse {
    table: String,
    offset: usize,
    limit: usize,
    total_rows: usize,
    columns: Vec<String>,
    rows: Vec<TableRowResponse>,
    editable: bool,
    key_columns: Vec<String>,
}
#[derive(Serialize)]
struct TableRowResponse { row_id: u64, values: Vec<JsonValue> }

impl From<QueryResult> for QueryResponse {
    fn from(result: QueryResult) -> Self {
        Self {
            columns: result.columns,
            rows: result.rows.into_iter()
                .map(|row| row.values.into_iter().map(|value| value.to_string()).collect())
                .collect(),
            affected_rows: result.affected_rows,
            message: result.message,
        }
    }
}
impl From<CatalogSnapshot> for CatalogResponse {
    fn from(catalog: CatalogSnapshot) -> Self {
        Self {
            tables: catalog.tables.into_iter().map(|table| TableResponse {
                name: table.name,
                row_count: table.row_count,
                columns: table.columns.into_iter().map(|column| ColumnResponse {
                    name: column.name,
                    data_type: format!("{:?}", column.data_type),
                    nullable: column.nullable,
                }).collect(),
                indexes: table.indexes.into_iter().map(|index| IndexResponse {
                    name: index.name, columns: index.columns, unique: index.unique,
                }).collect(),
            }).collect(),
        }
    }
}

fn value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Integer(value) => JsonValue::Number(value.into()),
        Value::Text(value) => JsonValue::String(value),
        Value::Boolean(value) => JsonValue::Bool(value),
    }
}
fn json_to_value(value: &JsonValue, data_type: &DataType, nullable: bool) -> Result<Value, String> {
    if value.is_null() {
        return if nullable { Ok(Value::Null) } else { Err("column does not allow NULL".into()) };
    }
    match data_type {
        DataType::Integer => value.as_i64().map(Value::Integer)
            .ok_or_else(|| "expected a signed 64-bit integer".into()),
        DataType::Text => value.as_str().map(|text| Value::Text(text.to_owned()))
            .ok_or_else(|| "expected text".into()),
        DataType::Boolean => value.as_bool().map(Value::Boolean)
            .ok_or_else(|| "expected true or false".into()),
    }
}
fn sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Integer(value) => value.to_string(),
        Value::Boolean(value) => if *value { "TRUE".into() } else { "FALSE".into() },
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
    }
}
fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
fn key_predicate(table: &TableInfo, key: &[JsonValue]) -> Result<String, String> {
    if !valid_identifier(&table.name) {
        return Err("table name is not a supported SQL identifier".into());
    }
    let unique_index = table.indexes.iter().find(|index| index.unique)
        .ok_or_else(|| "editing and deleting rows requires a UNIQUE index or PRIMARY KEY".to_owned())?;
    if key.len() != unique_index.columns.len() {
        return Err("row key does not match the table's UNIQUE index".into());
    }
    let mut terms = Vec::with_capacity(key.len());
    for (column_name, json_value) in unique_index.columns.iter().zip(key.iter()) {
        if !valid_identifier(column_name) {
            return Err("key column name is not a supported SQL identifier".into());
        }
        let column = table.columns.iter().find(|column| column.name == *column_name)
            .ok_or_else(|| format!("key column {column_name} is missing"))?;
        let value = json_to_value(json_value, &column.data_type, column.nullable)?;
        terms.push(format!("{column_name} = {}", sql_literal(&value)));
    }
    Ok(terms.join(" AND "))
}

fn catalog_snapshot(state: &AppState) -> Result<CatalogSnapshot, String> {
    let mut remote = state.remote.lock().map_err(|_| "remote connection lock is unavailable".to_owned())?;
    if let Some(client) = remote.as_mut() {
        return client.catalog().map_err(|error| error.to_string());
    }
    drop(remote);
    let database = state.database.lock().map_err(|_| "database lock is unavailable".to_owned())?;
    Ok(database.catalog())
}

fn table_info(state: &AppState, table_name: &str) -> Result<TableInfo, String> {
    let mut remote = state.remote.lock().map_err(|_| "remote connection lock is unavailable".to_owned())?;
    if let Some(client) = remote.as_mut() {
        let catalog = client.catalog().map_err(|error| error.to_string())?;
        return catalog.tables.into_iter().find(|table| table.name.eq_ignore_ascii_case(table_name))
            .ok_or_else(|| format!("table not found: {table_name}"));
    }
    drop(remote);
    state.database.lock().map_err(|_| "database lock is unavailable".to_owned())?
        .table_info(table_name).map_err(|error| error.to_string())
}

fn execute_statement(state: &AppState, sql: &str) -> Result<QueryResult, String> {
    let mut remote = state.remote.lock().map_err(|_| "remote connection lock is unavailable".to_owned())?;
    if let Some(client) = remote.as_mut() {
        return client.execute(sql).map_err(|error| error.to_string());
    }
    drop(remote);
    state.database.lock().map_err(|_| "database lock is unavailable".to_owned())?
        .execute(sql).map_err(|error| error.to_string())
}

async fn index() -> Html<&'static str> { Html(INDEX_HTML) }
async fn health(State(state): State<AppState>) -> Json<JsonValue> {
    let remote = state.remote.lock().ok().is_some_and(|value| value.is_some());
    Json(serde_json::json!({
        "status": "ok",
        "mode": if remote { "remote-tls" } else { "local-embedded" }
    }))
}

async fn remote_connect(
    State(state): State<AppState>,
    Json(request): Json<RemoteConnectRequest>,
) -> Response {
    if request.address.len() > 512 || request.server_name.len() > 253
        || request.ca_bundle_path.len() > 4096 || request.username.len() > 64
        || request.password.len() > 1024 {
        return (StatusCode::BAD_REQUEST, "remote connection field exceeds its configured limit").into_response();
    }
    if request.address.trim().is_empty() || request.server_name.trim().is_empty()
        || request.ca_bundle_path.trim().is_empty() || request.username.trim().is_empty()
        || request.password.is_empty() {
        return (StatusCode::BAD_REQUEST, "address, TLS name, CA bundle, username, and password are required").into_response();
    }
    let mut client = match RemoteClient::connect(
        request.address.trim(), request.server_name.trim(), request.ca_bundle_path.trim(),
        request.username.trim(), &request.password,
    ) {
        Ok(client) => client,
        Err(error) => return (StatusCode::BAD_GATEWAY, format!("TLS connection failed: {error}")).into_response(),
    };
    let catalog = match client.catalog() {
        Ok(catalog) => catalog,
        Err(error) => return (StatusCode::BAD_GATEWAY, format!("remote catalog failed: {error}")).into_response(),
    };
    let username = client.username().to_owned();
    let remote = match state.remote.lock() {
        Ok(remote) => remote,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "remote connection lock is unavailable").into_response(),
    };
    drop(remote);
    if let Ok(mut remote) = state.remote.lock() { *remote = Some(client); }
    Json(serde_json::json!({
        "connected": true,
        "mode": "remote-tls",
        "username": username,
        "catalog": CatalogResponse::from(catalog)
    })).into_response()
}

async fn remote_disconnect(State(state): State<AppState>) -> Response {
    match state.remote.lock() {
        Ok(mut remote) => {
            *remote = None;
            Json(serde_json::json!({"connected":false,"mode":"local-embedded"})).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "remote connection lock is unavailable").into_response(),
    }
}

async fn get_catalog(State(state): State<AppState>) -> Response {
    match catalog_snapshot(&state) {
        Ok(catalog) => Json(CatalogResponse::from(catalog)).into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, error).into_response(),
    }
}
async fn get_table_rows(
    State(state): State<AppState>, Path(table_name): Path<String>, Query(params): Query<PageParams>,
) -> Response {
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(50);
    if limit == 0 || limit > MAX_PAGE_SIZE {
        return (StatusCode::BAD_REQUEST, format!("page size must be 1..={MAX_PAGE_SIZE}")).into_response();
    }

    let mut remote = match state.remote.lock() {
        Ok(remote) => remote,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "remote connection lock is unavailable").into_response(),
    };
    let (info, result, total_rows) = if let Some(client) = remote.as_mut() {
        let catalog = match client.catalog() {
            Ok(catalog) => catalog,
            Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
        };
        let Some(info) = catalog.tables.into_iter().find(|table| table.name.eq_ignore_ascii_case(&table_name)) else {
            return (StatusCode::NOT_FOUND, format!("table not found: {table_name}")).into_response();
        };
        let page = match client.read_table_page(&info.name, offset, limit) {
            Ok(page) => page,
            Err(error) => return (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
        };
        (info, page.result, page.total_rows)
    } else {
        drop(remote);
        let database = match state.database.lock() {
            Ok(database) => database,
            Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "database lock is unavailable").into_response(),
        };
        let info = match database.table_info(&table_name) {
            Ok(info) => info,
            Err(error) => return (StatusCode::NOT_FOUND, error.to_string()).into_response(),
        };
        let result = match database.read_table_page(&table_name, offset, limit) {
            Ok(result) => result,
            Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
        };
        let total_rows = info.row_count;
        (info, result, total_rows)
    };
    let unique = info.indexes.iter().find(|index| index.unique);
    Json(TableRowsResponse {
        table: info.name, offset, limit, total_rows, columns: result.columns,
        rows: result.rows.into_iter().map(|row| TableRowResponse {
            row_id: row.row_id, values: row.values.into_iter().map(value_to_json).collect(),
        }).collect(),
        editable: unique.is_some(),
        key_columns: unique.map(|index| index.columns.clone()).unwrap_or_default(),
    }).into_response()
}

async fn insert_row(
    State(state): State<AppState>, Path(table_name): Path<String>, Json(request): Json<InsertRowRequest>,
) -> Response {
    if !valid_identifier(&table_name) {
        return (StatusCode::BAD_REQUEST, "table name is not a supported SQL identifier").into_response();
    }
    let info = match table_info(&state, &table_name) {
        Ok(info) => info,
        Err(error) => return (StatusCode::NOT_FOUND, error).into_response(),
    };
    if request.values.len() != info.columns.len() {
        return (StatusCode::BAD_REQUEST, "value count does not match the table column count").into_response();
    }
    let mut values = Vec::with_capacity(info.columns.len());
    for (column, json_value) in info.columns.iter().zip(request.values.iter()) {
        if !valid_identifier(&column.name) {
            return (StatusCode::BAD_REQUEST, "column name is not a supported SQL identifier").into_response();
        }
        match json_to_value(json_value, &column.data_type, column.nullable) {
            Ok(value) => values.push(value),
            Err(error) => return (StatusCode::BAD_REQUEST, format!("{}: {error}", column.name)).into_response(),
        }
    }
    let column_names = info.columns.iter().map(|column| column.name.clone()).collect::<Vec<_>>();
    let statement = format!("INSERT INTO {} ({}) VALUES ({})",
        info.name, column_names.join(", "), values.iter().map(sql_literal).collect::<Vec<_>>().join(", "));
    match execute_statement(&state, &statement) {
        Ok(result) => Json(QueryResponse::from(result)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

async fn update_cell(
    State(state): State<AppState>, Path(table_name): Path<String>, Json(request): Json<UpdateCellRequest>,
) -> Response {
    if !valid_identifier(&table_name) || !valid_identifier(&request.column) {
        return (StatusCode::BAD_REQUEST, "table or column name is not a supported SQL identifier").into_response();
    }
    let info = match table_info(&state, &table_name) {
        Ok(info) => info,
        Err(error) => return (StatusCode::NOT_FOUND, error).into_response(),
    };
    let column = match info.columns.iter().find(|column| column.name == request.column) {
        Some(column) => column,
        None => return (StatusCode::BAD_REQUEST, "column does not exist").into_response(),
    };
    let value = match json_to_value(&request.value, &column.data_type, column.nullable) {
        Ok(value) => value,
        Err(error) => return (StatusCode::BAD_REQUEST, format!("{}: {error}", column.name)).into_response(),
    };
    let predicate = match key_predicate(&info, &request.key) {
        Ok(value) => value,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let statement = format!("UPDATE {} SET {} = {} WHERE {predicate}", info.name, request.column, sql_literal(&value));
    match execute_statement(&state, &statement) {
        Ok(result) if result.affected_rows == 1 => Json(QueryResponse::from(result)).into_response(),
        Ok(result) => (StatusCode::CONFLICT, format!("expected one matching row, updated {}", result.affected_rows)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

async fn delete_row(
    State(state): State<AppState>, Path(table_name): Path<String>, Json(request): Json<DeleteRowRequest>,
) -> Response {
    let info = match table_info(&state, &table_name) {
        Ok(info) => info,
        Err(error) => return (StatusCode::NOT_FOUND, error).into_response(),
    };
    let predicate = match key_predicate(&info, &request.key) {
        Ok(value) => value,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    let statement = format!("DELETE FROM {} WHERE {predicate}", info.name);
    match execute_statement(&state, &statement) {
        Ok(result) if result.affected_rows == 1 => Json(QueryResponse::from(result)).into_response(),
        Ok(result) => (StatusCode::CONFLICT, format!("expected one matching row, deleted {}", result.affected_rows)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

async fn execute_query(State(state): State<AppState>, Json(request): Json<QueryRequest>) -> Response {
    if request.sql.trim().is_empty() || request.sql.len() > MAX_SQL_BYTES {
        return (StatusCode::BAD_REQUEST, "SQL query is empty or exceeds 64 KiB").into_response();
    }
    match execute_statement(&state, request.sql.trim()) {
        Ok(result) => Json(QueryResponse::from(result)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error).into_response(),
    }
}

async fn server_status(State(state): State<AppState>) -> Response {
    let mut remote = match state.remote.lock() {
        Ok(remote) => remote,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "remote connection lock is unavailable").into_response(),
    };
    if let Some(client) = remote.as_mut() {
        match client.server_status() {
            Ok(status) => Json(status).into_response(),
            Err(error) => (StatusCode::BAD_GATEWAY, error.to_string()).into_response(),
        }
    } else {
        Json(serde_json::json!({"mode":"local-embedded","remote":false})).into_response()
    }
}

async fn admin_action(
    State(state): State<AppState>,
    Json(request): Json<JsonValue>,
) -> Response {
    let mut remote = match state.remote.lock() {
        Ok(remote) => remote,
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "remote connection lock is unavailable").into_response(),
    };
    let Some(client) = remote.as_mut() else {
        return (StatusCode::FORBIDDEN, "user, role and audit administration is available only through an authenticated remote Admin session").into_response();
    };
    match client.admin_action(&request) {
        Ok(result) => Json(result).into_response(),
        Err(error) => (StatusCode::FORBIDDEN, error.to_string()).into_response(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let database_path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
    let address: SocketAddr = args.next().unwrap_or_else(|| "127.0.0.1:7480".into()).parse()?;
    if !address.ip().is_loopback() {
        return Err("PostemberDB Admin currently refuses non-loopback bind addresses".into());
    }
    let database = Database::open(&database_path)?;
    let state = AppState {
        database: Arc::new(Mutex::new(database)),
        remote: Arc::new(Mutex::new(None)),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/catalog", get(get_catalog))
        .route("/api/remote/connect", post(remote_connect))
        .route("/api/remote/disconnect", post(remote_disconnect))
        .route("/api/server/status", get(server_status))
        .route("/api/admin", post(admin_action))
        .route("/api/tables/:table/rows", get(get_table_rows).post(insert_row).patch(update_cell).delete(delete_row))
        .route("/api/query", post(execute_query))
        .layer(DefaultBodyLimit::max(256 * 1024))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("PostemberDB Admin is available at http://{address}");
    println!("Local database: {database_path}");
    println!("The web panel remains loopback-only; remote database sessions use verified TLS.");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_json_values_using_column_types_and_nullability() {
        assert_eq!(json_to_value(&serde_json::json!(42), &DataType::Integer, false).unwrap(), Value::Integer(42));
        assert_eq!(json_to_value(&serde_json::json!("hello"), &DataType::Text, false).unwrap(), Value::Text("hello".into()));
        assert_eq!(json_to_value(&serde_json::json!(true), &DataType::Boolean, false).unwrap(), Value::Boolean(true));
        assert_eq!(json_to_value(&JsonValue::Null, &DataType::Text, true).unwrap(), Value::Null);
        assert!(json_to_value(&JsonValue::Null, &DataType::Text, false).is_err());
        assert!(json_to_value(&serde_json::json!("42"), &DataType::Integer, false).is_err());
    }

    #[test]
    fn sql_text_literals_escape_quotes_and_identifiers_are_restricted() {
        assert_eq!(sql_literal(&Value::Text("O'Reilly".into())), "'O''Reilly'");
        assert!(valid_identifier("player_stats_2026"));
        assert!(!valid_identifier("../player_stats"));
        assert!(!valid_identifier("player;drop"));
    }

    #[test]
    fn row_key_predicate_targets_the_declared_unique_key() {
        let mut database = Database::new();
        database.execute("CREATE TABLE editor_rows (id INTEGER PRIMARY KEY, title TEXT)").unwrap();
        database.execute("INSERT INTO editor_rows VALUES (7, 'before')").unwrap();
        let info = database.table_info("editor_rows").unwrap();
        let predicate = key_predicate(&info, &[serde_json::json!(7)]).unwrap();
        assert_eq!(predicate, "id = 7");
        let updated = database.execute(&format!("UPDATE editor_rows SET title = 'after' WHERE {predicate}")).unwrap();
        assert_eq!(updated.affected_rows, 1);
        let page = database.read_table_page("editor_rows", 0, 10).unwrap();
        assert_eq!(page.rows[0].values[1], Value::Text("after".into()));
        let deleted = database.execute(&format!("DELETE FROM editor_rows WHERE {predicate}")).unwrap();
        assert_eq!(deleted.affected_rows, 1);
        assert!(database.read_table_page("editor_rows", 0, 10).unwrap().rows.is_empty());
    }

    #[test]
    fn row_edits_are_rejected_without_a_unique_index() {
        let mut database = Database::new();
        database.execute("CREATE TABLE no_key (note TEXT)").unwrap();
        let info = database.table_info("no_key").unwrap();
        assert!(key_predicate(&info, &[]).is_err());
    }
}
