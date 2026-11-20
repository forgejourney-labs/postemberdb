//! Authenticated TLS client for the PostemberDB Binary/1 protocol.
//!
//! Remote connections always validate a configured CA bundle and the server
//! name. There is deliberately no insecure certificate-validation switch.

use crate::protocol::{Frame, FrameKind, DEFAULT_MAX_PAYLOAD};
use crate::{CatalogSnapshot, QueryResult, Row, Value};
use rustls::{pki_types::ServerName, ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_GREETING_BYTES: usize = 4 * 1024;
const MAX_ADMIN_REQUEST_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTablePage {
    pub table: String,
    pub offset: usize,
    pub limit: usize,
    pub total_rows: usize,
    pub result: QueryResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteServerStatus {
    pub version: String,
    pub active_clients: usize,
    pub maximum_clients: usize,
    pub tls_enabled: bool,
    pub uptime_seconds: u64,
}

pub struct RemoteClient {
    reader: BufReader<StreamOwned<ClientConnection, TcpStream>>,
    next_request_id: u64,
    username: String,
}

impl RemoteClient {
    /// Connects through TLS, validates the certificate against the supplied CA
    /// bundle, authenticates, then negotiates the bounded Binary/1 frame protocol.
    pub fn connect(
        address: &str,
        server_name: &str,
        ca_bundle_path: impl AsRef<Path>,
        username: &str,
        password: &str,
    ) -> io::Result<Self> {
        if address.trim().is_empty() || server_name.trim().is_empty() {
            return Err(invalid_input("server address and TLS server name are required"));
        }
        if username.is_empty() || username.bytes().any(|byte| byte.is_ascii_whitespace()) {
            return Err(invalid_input("username must be a single non-empty token"));
        }
        if password.is_empty() || password.contains('\r') || password.contains('\n') {
            return Err(invalid_input("password must be non-empty and may not contain line breaks"));
        }
        if password.len() > 1024 {
            return Err(invalid_input("password exceeds the 1024-byte authentication limit"));
        }

        let mut ca_reader = BufReader::new(File::open(ca_bundle_path)?);
        let certificates = rustls_pemfile::certs(&mut ca_reader).collect::<Result<Vec<_>, _>>()?;
        if certificates.is_empty() {
            return Err(invalid_input("CA bundle contains no PEM certificates"));
        }
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate).map_err(|error| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("invalid CA certificate: {error}"))
            })?;
        }

        let server_name = ServerName::try_from(server_name.to_owned())
            .map_err(|_| invalid_input("TLS server name is invalid"))?;
        let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
        let connection = ClientConnection::new(Arc::new(config), server_name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;

        let address = address.to_socket_addrs()?.next()
            .ok_or_else(|| invalid_input("server address did not resolve"))?;
        let tcp = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)?;
        tcp.set_nodelay(true)?;
        tcp.set_read_timeout(Some(IO_TIMEOUT))?;
        tcp.set_write_timeout(Some(IO_TIMEOUT))?;
        let mut reader = BufReader::new(StreamOwned::new(connection, tcp));

        let greeting = read_bounded_line(&mut reader, MAX_GREETING_BYTES)?;
        if !greeting.starts_with("PostemberDB/") {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "server greeting is not recognized"));
        }

        write!(reader.get_mut(), "AUTH {username} {password}\n")?;
        reader.get_mut().flush()?;
        let authentication = read_bounded_line(&mut reader, MAX_GREETING_BYTES)?;
        if authentication != "OK authenticated" {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, authentication));
        }

        reader.get_mut().write_all(b"POSTEMBERDB BINARY/1\n")?;
        reader.get_mut().flush()?;
        let ready = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD)?;
        if ready.kind != FrameKind::Pong || ready.request_id != 0
            || ready.payload.as_slice() != b"postemberdb-binary/1-ready" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "server did not acknowledge Binary/1 negotiation"));
        }

        Ok(Self { reader, next_request_id: 1, username: username.to_owned() })
    }

    pub fn username(&self) -> &str { &self.username }

    fn request(&mut self, kind: FrameKind, expected: FrameKind, payload: Vec<u8>) -> io::Result<Vec<u8>> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1)
            .ok_or_else(|| io::Error::other("Binary/1 request ID space exhausted"))?;
        Frame::new(kind, request_id, payload).write_to(self.reader.get_mut(), DEFAULT_MAX_PAYLOAD)?;
        let response = Frame::read_from(&mut self.reader, DEFAULT_MAX_PAYLOAD)?;
        if response.request_id != request_id {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Binary/1 response request ID mismatch"));
        }
        if response.kind == FrameKind::Error {
            let message = String::from_utf8_lossy(&response.payload).into_owned();
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, message));
        }
        if response.kind != expected {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected Binary/1 response frame kind"));
        }
        Ok(response.payload)
    }

    pub fn ping(&mut self) -> io::Result<()> {
        self.request(FrameKind::Ping, FrameKind::Pong, b"ping".to_vec()).map(|_| ())
    }

    pub fn execute(&mut self, sql: &str) -> io::Result<QueryResult> {
        if sql.trim().is_empty() || sql.len() > DEFAULT_MAX_PAYLOAD {
            return Err(invalid_input("SQL query must contain 1..=8 MiB of UTF-8 text"));
        }
        let payload = self.request(FrameKind::Query, FrameKind::QueryResult, sql.as_bytes().to_vec())?;
        parse_query_result(&payload)
    }

    pub fn catalog(&mut self) -> io::Result<CatalogSnapshot> {
        let payload = self.request(FrameKind::Catalog, FrameKind::CatalogResult, Vec::new())?;
        serde_json::from_slice(&payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid catalog response: {error}")))
    }

    pub fn read_table_page(&mut self, table: &str, offset: usize, limit: usize) -> io::Result<RemoteTablePage> {
        let encoded = serde_json::to_vec(&json!({ "table": table, "offset": offset, "limit": limit }))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let payload = self.request(FrameKind::TablePage, FrameKind::TablePageResult, encoded)?;
        serde_json::from_slice(&payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid table page response: {error}")))
    }

    pub fn execute_batch(&mut self, statements: &[String]) -> io::Result<()> {
        if statements.is_empty() || statements.len() > 500
            || statements.iter().any(|statement| statement.trim().is_empty()) {
            return Err(invalid_input("transaction batch must contain 1..=500 non-empty statements"));
        }
        let encoded = serde_json::to_vec(&json!({ "statements": statements }))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        self.request(FrameKind::BatchQuery, FrameKind::BatchQueryResult, encoded).map(|_| ())
    }

    pub fn server_status(&mut self) -> io::Result<RemoteServerStatus> {
        let payload = self.request(FrameKind::ServerStatus, FrameKind::ServerStatusResult, Vec::new())?;
        serde_json::from_slice(&payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid server status response: {error}")))
    }

    /// Sends a bounded administrative command. The server independently checks
    /// that the authenticated identity has the Admin permission.
    pub fn admin_action(&mut self, request: &JsonValue) -> io::Result<JsonValue> {
        let encoded = serde_json::to_vec(request)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        if encoded.len() > MAX_ADMIN_REQUEST_BYTES {
            return Err(invalid_input("administrative request exceeds 64 KiB"));
        }
        let payload = self.request(FrameKind::AdminRequest, FrameKind::AdminResponse, encoded)?;
        serde_json::from_slice(&payload)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid administrative response: {error}")))
    }
}

fn parse_query_result(bytes: &[u8]) -> io::Result<QueryResult> {
    let value: JsonValue = serde_json::from_slice(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid SQL response: {error}")))?;
    if value.get("ok").and_then(JsonValue::as_bool) == Some(false) {
        let message = value.get("error").and_then(JsonValue::as_str).unwrap_or("remote SQL request failed");
        return Err(io::Error::new(io::ErrorKind::Other, message.to_owned()));
    }
    let columns = value.get("columns").and_then(JsonValue::as_array)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SQL response has no column array"))?
        .iter().map(|value| value.as_str().map(str::to_owned)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SQL column name is not text")))
        .collect::<io::Result<Vec<_>>>()?;
    let rows = value.get("rows").and_then(JsonValue::as_array)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SQL response has no row array"))?
        .iter().map(|row| {
            let values = row.as_array()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "SQL row is not an array"))?
                .iter().map(parse_value).collect::<io::Result<Vec<_>>>()?;
            Ok(Row { row_id: 0, values })
        }).collect::<io::Result<Vec<_>>>()?;
    let affected_rows = value.get("affected_rows").and_then(JsonValue::as_u64).unwrap_or(0) as usize;
    let message = value.get("message").and_then(JsonValue::as_str).unwrap_or("").to_owned();
    Ok(QueryResult { columns, rows, affected_rows, message })
}

fn parse_value(value: &JsonValue) -> io::Result<Value> {
    if value.is_null() { return Ok(Value::Null); }
    if let Some(integer) = value.as_i64() { return Ok(Value::Integer(integer)); }
    if let Some(boolean) = value.as_bool() { return Ok(Value::Boolean(boolean)); }
    if let Some(text) = value.as_str() { return Ok(Value::Text(text.to_owned())); }
    Err(io::Error::new(io::ErrorKind::InvalidData, "SQL value has an unsupported JSON type"))
}

fn read_bounded_line<R: BufRead>(reader: &mut R, maximum: usize) -> io::Result<String> {
    let mut output = Vec::with_capacity(128);
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed before line response"));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let content_length = newline.unwrap_or(consumed);
        if output.len().saturating_add(content_length) > maximum {
            reader.consume(consumed);
            return Err(invalid_input("server greeting exceeds configured line limit"));
        }
        output.extend_from_slice(&available[..content_length]);
        reader.consume(consumed);
        if newline.is_some() { break; }
    }
    if output.last() == Some(&b'\n') { output.pop(); }
    if output.last() == Some(&b'\r') { output.pop(); }
    String::from_utf8(output)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "server line is not valid UTF-8"))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typed_sql_result_values_and_metadata() {
        let payload = br#"{
            "ok": true,
            "columns": ["id", "name", "active", "note"],
            "rows": [
                [42, "Ada", true, null],
                [-7, "Grace", false, "legacy"]
            ],
            "affected_rows": 2,
            "message": "2 row(s)"
        }"#;

        let result = parse_query_result(payload).unwrap();
        assert_eq!(result.columns, vec!["id", "name", "active", "note"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].row_id, 0);
        assert_eq!(result.rows[0].values, vec![
            Value::Integer(42),
            Value::Text("Ada".into()),
            Value::Boolean(true),
            Value::Null,
        ]);
        assert_eq!(result.rows[1].values, vec![
            Value::Integer(-7),
            Value::Text("Grace".into()),
            Value::Boolean(false),
            Value::Text("legacy".into()),
        ]);
        assert_eq!(result.affected_rows, 2);
        assert_eq!(result.message, "2 row(s)");
    }

    #[test]
    fn rejects_remote_sql_errors_and_malformed_result_shapes() {
        let denied = br#"{"ok":false,"error":"not authorized"}"#;
        assert_eq!(
            parse_query_result(denied).unwrap_err().kind(),
            io::ErrorKind::Other,
        );

        let missing_columns = br#"{"rows":[],"affected_rows":0}"#;
        assert_eq!(
            parse_query_result(missing_columns).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
        );

        let invalid_value = br#"{"columns":["value"],"rows":[[{}]]}"#;
        assert_eq!(
            parse_query_result(invalid_value).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
        );

        let malformed_json = b"{";
        assert_eq!(
            parse_query_result(malformed_json).unwrap_err().kind(),
            io::ErrorKind::InvalidData,
        );
    }

    #[test]
    fn rejects_missing_remote_connection_fields_before_opening_ca_bundle() {
        let error = RemoteClient::connect(
            "",
            "localhost",
            "/path/that/does/not/exist/ca.pem",
            "admin",
            "a valid test password",
        ).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let error = RemoteClient::connect(
            "127.0.0.1:7432",
            "",
            "/path/that/does/not/exist/ca.pem",
            "admin",
            "a valid test password",
        ).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn rejects_line_breaks_and_oversized_authentication_tokens() {
        let error = RemoteClient::connect(
            "127.0.0.1:7432",
            "localhost",
            "/path/that/does/not/exist/ca.pem",
            "admin\nAUTH root anything",
            "valid password",
        ).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

        let error = RemoteClient::connect(
            "127.0.0.1:7432",
            "localhost",
            "/path/that/does/not/exist/ca.pem",
            "admin",
            &"p".repeat(1025),
        ).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
