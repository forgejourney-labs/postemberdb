use postemberdb::{Database, DatabaseSession, QueryResult, Value};
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const MAX_CLIENTS: usize = 128;
const MAX_SQL_LINE_BYTES: usize = 64 * 1024;
const CLIENT_IO_TIMEOUT_SECS: u64 = 30;

struct ConnectionGuard(Arc<AtomicUsize>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_bounded_line<R: io::BufRead>(reader: &mut R, output: &mut Vec<u8>, limit: usize) -> io::Result<Option<usize>> {
    output.clear();
    let mut overflow = false;
    let mut received_any = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if !received_any { return Ok(None); }
            break;
        }
        received_any = true;
        let newline = available.iter().position(|byte| *byte == 10);
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let content_len = newline.unwrap_or(consumed);
        if !overflow {
            if output.len().saturating_add(content_len) > limit {
                overflow = true;
            } else {
                output.extend_from_slice(&available[..content_len]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() { break; }
    }
    if overflow {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "SQL line exceeds configured limit"));
    }
    if output.last() == Some(&13) { output.pop(); }
    Ok(Some(output.len()))
}

fn validate_bind_address(address: &str) -> io::Result<SocketAddr> {
    let socket_address: SocketAddr = address.parse().map_err(|_| io::Error::new(
        io::ErrorKind::InvalidInput,
        "server address must be an explicit loopback socket address until TLS and authentication are implemented",
    ))?;
    if !socket_address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "refusing non-loopback bind: this development protocol has no TLS or authentication",
        ));
    }
    Ok(socket_address)
}

pub(crate) fn run_server(address: &str, data_path: &str) -> io::Result<()> {
    let socket_address = validate_bind_address(address)?;
    let listener = TcpListener::bind(socket_address)?;
    println!("PostemberDB development server listening on loopback {socket_address}");
    let database = Database::open(data_path)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
    let database = Arc::new(std::sync::Mutex::new(database));
    let active_clients = Arc::new(AtomicUsize::new(0));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let mut current = active_clients.load(Ordering::Acquire);
                loop {
                    if current >= MAX_CLIENTS {
                        eprintln!("connection rejected: maximum of {MAX_CLIENTS} clients reached");
                        drop(stream);
                        break;
                    }
                    match active_clients.compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            let db = database.clone();
                            let active = active_clients.clone();
                            let active_on_failure = active_clients.clone();
                            match thread::Builder::new()
                                .name("postemberdb-client".into())
                                .spawn(move || {
                                    let _guard = ConnectionGuard(active);
                                    if let Err(error) = handle_client(stream, db) {
                                        eprintln!("client error: {error}");
                                    }
                                })
                            {
                                Ok(_) => {}
                                Err(error) => {
                                    active_on_failure.fetch_sub(1, Ordering::AcqRel);
                                    eprintln!("could not start client worker: {error}");
                                }
                            }
                            break;
                        }
                        Err(observed) => current = observed,
                    }
                }
            }
            Err(error) => eprintln!("connection failed: {error}"),
        }
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    database: std::sync::Arc<std::sync::Mutex<Database>>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS)))?;
    let reader_stream = stream.try_clone()?;
    let mut reader = io::BufReader::new(reader_stream);
    let mut line = Vec::with_capacity(1024);
    let mut session: Option<DatabaseSession> = None;
    let mut jsonl_mode = false;
    writeln!(stream, "PostemberDB/0.1 ready; send one SQL statement per line")?;
    loop {
        let sql = match read_bounded_line(&mut reader, &mut line, MAX_SQL_LINE_BYTES) {
            Ok(Some(_)) => std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "SQL input must be valid UTF-8"))?.to_owned(),
            Ok(None) => break,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                writeln!(stream, "ERROR {error}")?;
                break;
            }
            Err(error) => return Err(error),
        };
        let command = sql.trim();
        if command.eq_ignore_ascii_case("QUIT") { break; }
        if command.eq_ignore_ascii_case("POSTEMBERDB JSONL/1") {
            jsonl_mode = true;
            stream.write_all(b"{\"protocol\":\"postemberdb-jsonl\",\"version\":1,\"ready\":true}\n")?;
            stream.flush()?;
            continue;
        }

        let response = if command.eq_ignore_ascii_case("BEGIN") || command.eq_ignore_ascii_case("BEGIN TRANSACTION") {
            if session.is_some() {
                if jsonl_mode { json_error("transaction already active") } else { "ERROR transaction already active\n".to_owned() }
            } else {
                match database.lock() {
                    Ok(db) => {
                        session = Some(db.begin_session());
                        if jsonl_mode { json_ok_message("transaction started") } else { "OK transaction started\n".to_owned() }
                    }
                    Err(_) => if jsonl_mode { json_error("database lock poisoned") } else { "ERROR database lock poisoned\n".to_owned() },
                }
            }
        } else if command.eq_ignore_ascii_case("COMMIT") {
            match session.take() {
                None => if jsonl_mode { json_error("no active transaction") } else { "ERROR no active transaction\n".to_owned() },
                Some(transaction) => match database.lock() {
                    Ok(mut db) => match db.commit_session(transaction) {
                        Ok(()) => if jsonl_mode { json_ok_message("transaction committed") } else { "OK transaction committed\n".to_owned() },
                        Err(error) => if jsonl_mode { json_error(&error.to_string()) } else { format!("ERROR {error}\n") },
                    },
                    Err(_) => if jsonl_mode { json_error("database lock poisoned") } else { "ERROR database lock poisoned\n".to_owned() },
                }
            }
        } else if command.eq_ignore_ascii_case("ROLLBACK") {
            if session.take().is_some() {
                if jsonl_mode { json_ok_message("transaction rolled back") } else { "OK transaction rolled back\n".to_owned() }
            } else {
                if jsonl_mode { json_error("no active transaction") } else { "ERROR no active transaction\n".to_owned() }
            }
        } else if let Some(transaction) = session.as_mut() {
            match transaction.execute(&sql) {
                Ok(result) => if jsonl_mode { format_result_json(result) } else { format_result(result) },
                Err(error) => if jsonl_mode { json_error(&error.to_string()) } else { format!("ERROR {error}\n") },
            }
        } else {
            match database.lock() {
                Ok(mut db) => match db.execute(&sql) {
                    Ok(result) => if jsonl_mode { format_result_json(result) } else { format_result(result) },
                    Err(error) => if jsonl_mode { json_error(&error.to_string()) } else { format!("ERROR {error}\n") },
                },
                Err(_) => if jsonl_mode { json_error("database lock poisoned") } else { "ERROR database lock poisoned\n".to_owned() },
            }
        };
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
    }
    // An uncommitted session is dropped and therefore rolled back on disconnect.
    Ok(())
}

fn json_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character <= '\u{1f}' => output.push_str(&format!("\\u{:04x}", character as u32)),
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn json_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Integer(number) => number.to_string(),
        Value::Boolean(boolean) => boolean.to_string(),
        Value::Text(text) => json_escape(text),
    }
}

fn json_ok_message(message: &str) -> String {
    format!("{{\"ok\":true,\"message\":{},\"affected_rows\":0,\"columns\":[],\"rows\":[]}}\n", json_escape(message))
}

fn json_error(message: &str) -> String {
    format!("{{\"ok\":false,\"error\":{}}}\n", json_escape(message))
}

fn format_result_json(result: QueryResult) -> String {
    let columns = result.columns.iter().map(|column| json_escape(column)).collect::<Vec<_>>().join(",");
    let rows = result.rows.iter().map(|row| {
        let values = row.values.iter().map(json_value).collect::<Vec<_>>().join(",");
        format!("[{values}]")
    }).collect::<Vec<_>>().join(",");
    format!(
        "{{\"ok\":true,\"message\":{},\"affected_rows\":{},\"columns\":[{}],\"rows\":[{}]}}\n",
        json_escape(&result.message), result.affected_rows, columns, rows
    )
}

fn format_result(result: QueryResult) -> String {
    if result.columns.is_empty() {
        return format!("OK {}\n", result.message);
    }
    let mut output = format!("OK {}\n", result.message);
    for column in &result.columns { output.push_str(&format!("{column}\t")); }
    output.push('\n');
    for row in result.rows {
        for value in row.values { output.push_str(&format!("{value}\t")); }
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{read_bounded_line, validate_bind_address};
    use postemberdb::{QueryResult, Value};
    use std::io::{self, BufRead, BufReader, Cursor, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn jsonl_response_encodes_text_and_preserves_types() {
        let result = QueryResult {
            columns: vec!["name".into(), "count".into(), "enabled".into(), "missing".into()],
            rows: vec![postemberdb::Row { row_id: 0, values: vec![
                Value::Text("a\n\"b".into()), Value::Integer(42),
                Value::Boolean(true), Value::Null,
            ]}],
            affected_rows: 0,
            message: "selected".into(),
        };
        let encoded = super::format_result_json(result);
        assert!(encoded.contains("\"count\""));
        assert!(encoded.contains(",42,true,null]"));
        assert!(encoded.contains("a\\n\\\"b"));
        assert!(super::json_error("bad").contains("\"ok\":false"));
    }

    #[test]
    fn jsonl_protocol_handshake_and_sql_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let database = Arc::new(Mutex::new(postemberdb::Database::new()));
        let worker = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            super::handle_client(stream, database).unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        let reader_stream = client.try_clone().unwrap();
        let mut reader = BufReader::new(reader_stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("PostemberDB/0.1 ready"));

        writeln!(client, "POSTEMBERDB JSONL/1").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("postemberdb-jsonl"));
        assert!(line.contains("version"));

        writeln!(client, "CREATE TABLE ksl_test (id INTEGER, name TEXT)").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("ok"));

        writeln!(client, "INSERT INTO ksl_test VALUES (7, 'Ada')").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("ok"));

        writeln!(client, "SELECT id, name FROM ksl_test WHERE id = 7").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("Ada"));

        writeln!(client, "QUIT").unwrap();
        drop(client);
        worker.join().unwrap();
    }

    #[test]
    fn server_accepts_only_loopback_bind_addresses() {
        assert!(validate_bind_address("127.0.0.1:7432").is_ok());
        assert!(validate_bind_address("[::1]:7432").is_ok());
        assert_eq!(
            validate_bind_address("0.0.0.0:7432").unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            validate_bind_address("db.example.com:7432").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn bounded_line_reader_rejects_oversize_and_recovers_at_next_line() {
        let input = Cursor::new(vec![b'1', b'2', b'3', b'4', b'5', 10, b'O', b'K', 10]);
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        let error = read_bounded_line(&mut reader, &mut line, 4).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(2));
        assert_eq!(line, b"OK");
    }

    #[test]
    fn bounded_line_reader_accepts_exact_limit_and_final_line_without_newline() {
        let input = Cursor::new(vec![b'A', b'B', b'C', b'D', 10, b'x', b'y', b'z']);
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(4));
        assert_eq!(line, b"ABCD");
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(3));
        assert_eq!(line, b"xyz");
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), None);
    }
}
