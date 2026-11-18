use postemberdb::Database;
use std::io::{self, BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => {
            let address = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:7432");
            if let Err(error) = run_server(address) {
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
            eprintln!("  postemberdb server [host:port]");
            eprintln!("  postemberdb version");
        }
    }
}

fn run_server(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address)?;
    println!("PostemberDB server listening on {address}");
    let database = std::sync::Arc::new(std::sync::Mutex::new(Database::new()));

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
    writeln!(stream, "PostemberDB/0.1 ready; send one SQL statement per line")?;
    for line in reader.lines() {
        let sql = line?;
        if sql.trim().eq_ignore_ascii_case("QUIT") { break; }
        let response = match database.lock() {
            Ok(mut db) => match db.execute(&sql) {
                Ok(result) if !result.columns.is_empty() => {
                    let mut output = format!("OK {}\n", result.message);
                    for column in &result.columns { output.push_str(&format!("{column}\t")); }
                    output.push('\n');
                    for row in result.rows {
                        for value in row.values { output.push_str(&format!("{value}\t")); }
                        output.push('\n');
                    }
                    output
                }
                Ok(result) => format!("OK {}\n", result.message),
                Err(error) => format!("ERROR {error}\n"),
            },
            Err(_) => "ERROR database lock poisoned\n".to_owned(),
        };
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
    }
    Ok(())
}
