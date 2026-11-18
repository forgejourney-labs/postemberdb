use postemberdb::{Database, DatabaseSession, QueryResult};
use std::io::{self, BufRead, Write};
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
        let newline = available.iter().position(|byte| *byte == b'\\n');
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
    if output.last() == Some(&b'\\r') { output.pop(); }
    Ok(Some(output.len()))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => {
            let address = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:7432");
            let data_path = args.get(3).map(String::as_str).unwrap_or("postemberdb.pdb");
            if let Err(error) = run_server(address, data_path) {
                eprintln!("PostemberDB server error: {error}");
                std::process::exit(1);
            }
        }
        Some("version") | Some("--version") | Some("-V") => {
            println!("PostemberDB {}", env!("CARGO_PKG_VERSION"));
        }
        _ => {
            eprintln!("PostemberDB {}", env!("CARGO_PKG_VERSION"));
            eprintln!("Usage:");
            eprintln!("  postemberdb server [host:port] [data-file]");
            eprintln!("  postemberdb version");
        }
    }
}

fn run_server(address: &str, data_path: &str) -> io::Result<()> {
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
                            thread::spawn(move || {
                                let _guard = ConnectionGuard(active);
                                if let Err(error) = handle_client(stream, db) {
                                    eprintln!("client error: {error}");
                                }
                            });
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
    stream.set_read_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS)))?;
    let reader_stream = stream.try_clone()?;
    let mut reader = io::BufReader::new(reader_stream);
    let mut line = Vec::with_capacity(1024);
    let mut session: Option<DatabaseSession> = None;
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

        let response = if command.eq_ignore_ascii_case("BEGIN") || command.eq_ignore_ascii_case("BEGIN TRANSACTION") {
            if session.is_some() {
                "ERROR transaction already active\n".to_owned()
            } else {
                match database.lock() {
                    Ok(db) => {
                        session = Some(db.begin_session());
                        "OK transaction started\n".to_owned()
                    }
                    Err(_) => "ERROR database lock poisoned\n".to_owned(),
                }
            }
        } else if command.eq_ignore_ascii_case("COMMIT") {
            match session.take() {
                None => "ERROR no active transaction\n".to_owned(),
                Some(transaction) => match database.lock() {
                    Ok(mut db) => match db.commit_session(transaction) {
                        Ok(()) => "OK transaction committed\n".to_owned(),
                        Err(error) => format!("ERROR {error}\n"),
                    },
                    Err(_) => "ERROR database lock poisoned\n".to_owned(),
                }
            }
        } else if command.eq_ignore_ascii_case("ROLLBACK") {
            if session.take().is_some() {
                "OK transaction rolled back\n".to_owned()
            } else {
                "ERROR no active transaction\n".to_owned()
            }
        } else if let Some(transaction) = session.as_mut() {
            match transaction.execute(&sql) {
                Ok(result) => format_result(result),
                Err(error) => format!("ERROR {error}\n"),
            }
        } else {
            match database.lock() {
                Ok(mut db) => match db.execute(&sql) {
                    Ok(result) => format_result(result),
                    Err(error) => format!("ERROR {error}\n"),
                },
                Err(_) => "ERROR database lock poisoned\n".to_owned(),
            }
        };
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
    }
    // An uncommitted session is dropped and therefore rolled back on disconnect.
    Ok(())
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
    use std::io::{self, BufReader, Cursor};

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
        let input = Cursor::new(b"12345\\nOK\\n".to_vec());
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        let error = read_bounded_line(&mut reader, &mut line, 4).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(2));
        assert_eq!(line, b"OK");
    }

    #[test]
    fn bounded_line_reader_accepts_exact_limit_and_final_line_without_newline() {
        let input = Cursor::new(b"ABCD\\nxyz".to_vec());
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(4));
        assert_eq!(line, b"ABCD");
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), Some(3));
        assert_eq!(line, b"xyz");
        assert_eq!(read_bounded_line(&mut reader, &mut line, 4).unwrap(), None);
    }
}
