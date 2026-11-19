use postemberdb::{Database, DatabaseSession, EmberForge7PlayerProfile, EmberForge7Store, QueryResult, Value};
use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

const MAX_CLIENTS: usize = 128;
const MAX_SQL_LINE_BYTES: usize = 64 * 1024;
const CLIENT_IO_TIMEOUT_SECS: u64 = 30;
const MAX_EF7_WIRE_CUSTOM_BYTES: usize = 24 * 1024;

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
    let ef7_store = EmberForge7Store::open(data_path, 256)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
    let ef7_store = Arc::new(std::sync::Mutex::new(ef7_store));
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
                            let ef7 = ef7_store.clone();
                            let active = active_clients.clone();
                            let active_on_failure = active_clients.clone();
                            match thread::Builder::new()
                                .name("postemberdb-client".into())
                                .spawn(move || {
                                    let _guard = ConnectionGuard(active);
                                    if let Err(error) = handle_client(stream, db, ef7) {
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
    ef7: std::sync::Arc<std::sync::Mutex<EmberForge7Store>>,
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

        if jsonl_mode && command.starts_with("EF7 ") {
            let response = if session.is_some() {
                json_error("EF7 commands cannot run inside a SQL transaction; EF7 is a separate storage domain")
            } else {
                match ef7.lock() {
                    Ok(mut store) => handle_ef7_command(command, &mut store),
                    Err(_) => json_error("EF7 storage lock poisoned"),
                }
            };
            stream.write_all(response.as_bytes())?;
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


fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 || !value.is_ascii() {
        return Err("hex data must contain an even number of ASCII digits".into());
    }
    let mut output = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for pair in bytes.chunks_exact(2) {
        let digit = |byte: u8| -> Option<u8> {
            match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            }
        };
        let high = digit(pair[0]).ok_or_else(|| "invalid hexadecimal data".to_owned())?;
        let low = digit(pair[1]).ok_or_else(|| "invalid hexadecimal data".to_owned())?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_uuid(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32 {
        return Err("UUID must be 32 hexadecimal characters without hyphens".into());
    }
    decode_hex(value)?.try_into().map_err(|_| "UUID must decode to exactly 16 bytes".into())
}

fn json_profile(row_id: u64, profile: &EmberForge7PlayerProfile) -> Result<String, String> {
    if profile.custom_data.len() > MAX_EF7_WIRE_CUSTOM_BYTES {
        return Err(format!(
            "profile custom_data exceeds the JSONL/1 bridge limit of {MAX_EF7_WIRE_CUSTOM_BYTES} bytes; use the Rust API for larger profiles"
        ));
    }
    Ok(format!(
        "{{\"row_id\":{row_id},\"uuid\":{},\"username\":{},\"first_joined_ms\":{},\"last_seen_ms\":{},\"playtime_ticks\":{},\"custom_data_hex\":{}}}",
        json_escape(&encode_hex(&profile.uuid)),
        json_escape(&profile.username),
        profile.first_joined_ms,
        profile.last_seen_ms,
        profile.playtime_ticks,
        json_escape(&encode_hex(&profile.custom_data)),
    ))
}

fn handle_ef7_command(command: &str, store: &mut EmberForge7Store) -> String {
    let parts = command.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 2 || parts[0] != "EF7" {
        return json_error("invalid EF7 command");
    }
    let parse_id = |index: usize, label: &str| -> Result<u64, String> {
        parts.get(index).ok_or_else(|| format!("missing {label}"))?
            .parse::<u64>().map_err(|_| format!("invalid {label}"))
    };
    let result: Result<String, String> = (|| {
        match parts[1] {
            "PROFILE_PUT" => {
                if parts.len() != 10 { return Err("usage: EF7 PROFILE_PUT <namespace> <row_id> <uuid_hex> <username> <first_joined_ms> <last_seen_ms> <playtime_ticks> <custom_hex|->".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let row_id = parse_id(3, "row ID")?;
                let uuid = decode_uuid(parts[4])?;
                let first_joined_ms = parse_id(6, "first_joined_ms")?;
                let last_seen_ms = parse_id(7, "last_seen_ms")?;
                let playtime_ticks = parse_id(8, "playtime_ticks")?;
                let custom_data = if parts[9] == "-" { Vec::new() } else { decode_hex(parts[9])? };
                if custom_data.len() > MAX_EF7_WIRE_CUSTOM_BYTES {
                    return Err(format!("custom_data exceeds the JSONL/1 bridge limit of {MAX_EF7_WIRE_CUSTOM_BYTES} bytes"));
                }
                let profile = EmberForge7PlayerProfile {
                    uuid, username: parts[5].to_owned(), first_joined_ms, last_seen_ms,
                    playtime_ticks, custom_data,
                };
                store.put_player_profile(namespace, row_id, &profile)
                    .map_err(|error| error.to_string())?;
                Ok(json_ok_message("EF7 player profile stored"))
            }

            "PROFILE_BATCH" => {
                if parts.len() < 5 { return Err("usage: EF7 PROFILE_BATCH <namespace> <upsert_count> <remove_count> ...".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let upsert_count = parts[3].parse::<usize>().map_err(|_| "invalid upsert count".to_owned())?;
                let remove_count = parts[4].parse::<usize>().map_err(|_| "invalid removal count".to_owned())?;
                let total = upsert_count.checked_add(remove_count).ok_or_else(|| "profile batch count overflow".to_owned())?;
                if total > 2_500 {
                    return Err("EF7 profile batch exceeds 2,500 operations".into());
                }
                let expected_len = 5usize.checked_add(upsert_count.checked_mul(7).ok_or_else(|| "profile batch token count overflow".to_owned())?)
                    .and_then(|value| value.checked_add(remove_count.checked_mul(2)?))
                    .ok_or_else(|| "profile batch token count overflow".to_owned())?;
                if parts.len() != expected_len {
                    return Err("EF7 profile batch token count does not match its operation counts".into());
                }
                let mut upserts = Vec::with_capacity(upsert_count);
                let mut removals = Vec::with_capacity(remove_count);
                let mut offset = 5usize;
                for _ in 0..upsert_count {
                    let row_id = parts[offset].parse::<u64>().map_err(|_| "invalid profile row ID".to_owned())?;
                    let uuid = decode_uuid(parts[offset + 1])?;
                    let username = parts[offset + 2].to_owned();
                    let first_joined_ms = parts[offset + 3].parse::<u64>().map_err(|_| "invalid first_joined_ms".to_owned())?;
                    let last_seen_ms = parts[offset + 4].parse::<u64>().map_err(|_| "invalid last_seen_ms".to_owned())?;
                    let playtime_ticks = parts[offset + 5].parse::<u64>().map_err(|_| "invalid playtime_ticks".to_owned())?;
                    let custom_data = if parts[offset + 6] == "-" { Vec::new() } else { decode_hex(parts[offset + 6])? };
                    if custom_data.len() > MAX_EF7_WIRE_CUSTOM_BYTES {
                        return Err(format!("custom_data exceeds the JSONL/1 bridge limit of {MAX_EF7_WIRE_CUSTOM_BYTES} bytes"));
                    }
                    upserts.push((namespace, row_id, EmberForge7PlayerProfile {
                        uuid, username, first_joined_ms, last_seen_ms, playtime_ticks, custom_data,
                    }));
                    offset += 7;
                }
                for _ in 0..remove_count {
                    let row_id = parts[offset].parse::<u64>().map_err(|_| "invalid removed player row ID".to_owned())?;
                    let uuid = decode_uuid(parts[offset + 1])?;
                    removals.push((namespace, row_id, uuid));
                    offset += 2;
                }
                let (removed_keys, inserted_keys) = store.apply_player_profile_batch(&upserts, &removals)
                    .map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 profile batch committed\",\"affected_rows\":{total},\"index_entries_removed\":{removed_keys},\"index_entries_written\":{inserted_keys}}}\n"
                ))
            }
            "PROFILE_GET" => {
                if parts.len() != 4 { return Err("usage: EF7 PROFILE_GET <namespace> <uuid_hex>".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let uuid = decode_uuid(parts[3])?;
                match store.get_player_profile(namespace, uuid).map_err(|error| error.to_string())? {
                    Some((row_id, profile)) => {
                        let profile_json = json_profile(row_id, &profile)?;
                        Ok(format!("{{\"ok\":true,\"message\":\"profile found\",\"profile\":{profile_json}}}\n"))
                    }
                    None => Ok("{\"ok\":true,\"message\":\"profile not found\",\"profile\":null}\n".into()),
                }
            }
            "PROFILE_GET_NAME" => {
                if parts.len() != 4 { return Err("usage: EF7 PROFILE_GET_NAME <namespace> <username>".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                match store.get_player_profile_by_name(namespace, parts[3]).map_err(|error| error.to_string())? {
                    Some((row_id, profile)) => {
                        let profile_json = json_profile(row_id, &profile)?;
                        Ok(format!("{{\"ok\":true,\"message\":\"profile found\",\"profile\":{profile_json}}}\n"))
                    }
                    None => Ok("{\"ok\":true,\"message\":\"profile not found\",\"profile\":null}\n".into()),
                }
            }
            "PLAYER_DELETE" => {
                if parts.len() != 5 { return Err("usage: EF7 PLAYER_DELETE <namespace> <row_id> <uuid_hex>".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let row_id = parse_id(3, "row ID")?;
                let uuid = decode_uuid(parts[4])?;
                let deleted = store.delete_player(namespace, row_id, uuid)
                    .map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":{},\"affected_rows\":{},\"profile\":null}}\n",
                    json_escape(if deleted { "player deleted" } else { "player not found" }),
                    u8::from(deleted),
                ))
            }
            _ => Err("unsupported EF7 command".into()),
        }
    })();
    match result {
        Ok(response) => response,
        Err(error) => json_error(&error),
    }
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
    use postemberdb::{EmberForge7Store, QueryResult, Value};
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
        let ef7_path = std::env::temp_dir().join(format!(
            "postemberdb-jsonl-ef7-{}-{}.pbd", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let ef7 = Arc::new(Mutex::new(EmberForge7Store::open(&ef7_path, 16).unwrap()));
        let worker = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            super::handle_client(stream, database, ef7).unwrap();
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

        let uuid = "11111111111111111111111111111111";
        writeln!(client, "EF7 PROFILE_PUT 7 41 {uuid} Steve 100 200 300 01020304").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 player profile stored"));
        writeln!(client, "BEGIN").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("transaction started"));
        writeln!(client, "EF7 PROFILE_GET 7 {uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("cannot run inside a SQL transaction"));
        writeln!(client, "ROLLBACK").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("transaction rolled back"));
        writeln!(client, "EF7 PROFILE_GET 7 invalid").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("32 hexadecimal characters"));
        writeln!(client, "EF7 PROFILE_GET 7 {uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"username\":\"Steve\""));
        assert!(line.contains("\"custom_data_hex\":\"01020304\""));
        writeln!(client, "EF7 PROFILE_GET_NAME 7 STEVE").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"row_id\":41"));
        let removed_uuid = "33333333333333333333333333333333";
        writeln!(client, "EF7 PROFILE_PUT 7 42 {removed_uuid} Notch 110 210 310 0708").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 player profile stored"));
        let new_uuid = "22222222222222222222222222222222";
        writeln!(client, "EF7 PROFILE_BATCH 7 1 1 41 {new_uuid} Alex 100 250 400 0506 42 {removed_uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 profile batch committed"));
        writeln!(client, "EF7 PROFILE_GET 7 {uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"profile\":null"));
        writeln!(client, "EF7 PROFILE_GET_NAME 7 ALEX").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"username\":\"Alex\""));
        writeln!(client, "EF7 PROFILE_GET 7 {removed_uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"profile\":null"));
        writeln!(client, "EF7 PLAYER_DELETE 7 41 {new_uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("player deleted"));
        writeln!(client, "EF7 PROFILE_GET 7 {uuid}").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"profile\":null"));

        writeln!(client, "QUIT").unwrap();
        drop(client);
        worker.join().unwrap();
        for suffix in [".ef7.lock", ".ef7.pages", ".ef7.wal"] {
            let mut sidecar = ef7_path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(sidecar));
        }
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
