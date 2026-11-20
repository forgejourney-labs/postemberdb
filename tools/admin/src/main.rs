use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use postemberdb::{Database, QueryResult};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::{Arc, Mutex}};

const INDEX_HTML: &str = include_str!("../static/index.html");

#[derive(Clone)]
struct AppState { database: Arc<Mutex<Database>> }

#[derive(Deserialize)]
struct QueryRequest { sql: String }

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    affected_rows: usize,
    message: String,
}

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

async fn index() -> Html<&'static str> { Html(INDEX_HTML) }

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "mode": "local-embedded" }))
}

async fn execute_query(State(state): State<AppState>, Json(request): Json<QueryRequest>) -> Response {
    if request.sql.trim().is_empty() || request.sql.len() > 64 * 1024 {
        return (StatusCode::BAD_REQUEST, "SQL query is empty or exceeds 64 KiB").into_response();
    }
    let result = {
        let Ok(mut database) = state.database.lock() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "database lock is unavailable").into_response();
        };
        database.execute(request.sql.trim())
    };
    match result {
        Ok(result) => Json(QueryResponse::from(result)).into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let database_path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
    let address: SocketAddr = args.next()
        .unwrap_or_else(|| "127.0.0.1:7480".into()).parse()?;
    if !address.ip().is_loopback() {
        return Err("PostemberDB Admin currently refuses non-loopback bind addresses".into());
    }

    let database = Database::open(&database_path)?;
    let state = AppState { database: Arc::new(Mutex::new(database)) };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/query", post(execute_query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("PostemberDB Admin is available at http://{address}");
    println!("Local database: {database_path}");
    axum::serve(listener, app).await?;
    Ok(())
}
