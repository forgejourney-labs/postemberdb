use postemberdb::{Database, DatabaseSession, QueryResult};
use std::io::{self, BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

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
    let listener = TcpListener::bind(address)?;
    println!("PostemberDB server listening on {address}");
    let database = Database::open(data_path)
        .map_err(|error| io::Error::new(io::ErrorKind::Other, error.to_string()))?;
    let database = std::sync::Arc::new(std::sync::Mutex::new(database));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let db = database.clone();
                thread::spawn(move || {
                    if let Err(error) = handle_client(stream, db) {
                        eprintln!("client error: {error}");
                    }
                });
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
    let reader_stream = stream.try_clone()?;
    let reader = io::BufReader::new(reader_stream);
    let mut session: Option<DatabaseSession> = None;
    writeln!(stream, "PostemberDB/0.1 ready; send one SQL statement per line")?;
    for line in reader.lines() {
        let sql = line?;
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
