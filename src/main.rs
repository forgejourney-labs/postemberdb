mod server;

use postemberdb::version::version_string;
use postemberdb::Database;
use std::path::Path;

fn main() {
    let mut args = std::env::args().skip(1);
    let command = args.next();
    let result = match command.as_deref() {
        Some("server") => {
            let address = args.next().unwrap_or_else(|| "127.0.0.1:7432".into());
            let data_path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
            server::run_server(&address, &data_path)
        }
        Some("check") => {
            let path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
            run_check(&path)
        }
        Some("checkpoint") => {
            let path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
            run_checkpoint(&path)
        }
        Some("version") | Some("--version") | Some("-V") => {
            println!("{}", version_string());
            Ok(())
        }
        Some("help") | Some("--help") | Some("-h") | None => {
            print_usage();
            Ok(())
        }
        Some(other) => {
            eprintln!("unknown command: {other}");
            print_usage();
            std::process::exit(2);
        }
    };
    if let Err(error) = result {
        eprintln!("PostemberDB error: {error}");
        std::process::exit(1);
    }
}

fn require_existing_file(path: &str) -> std::io::Result<()> {
    if !Path::new(path).is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("database file not found: {path}"),
        ));
    }
    Ok(())
}

fn run_check(path: &str) -> std::io::Result<()> {
    require_existing_file(path)?;
    let _database = Database::open(path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    println!("OK: snapshot decoded and WAL recovery completed for {path}");
    println!("Note: check opens the database and may truncate an incomplete final WAL frame.");
    Ok(())
}

fn run_checkpoint(path: &str) -> std::io::Result<()> {
    require_existing_file(path)?;
    let database = Database::open(path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    database.checkpoint()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    println!("OK: checkpoint completed for {path}");
    Ok(())
}

fn print_usage() {
    eprintln!("{}", version_string());
    eprintln!("Usage:");
    eprintln!("  postemberdb server [127.0.0.1:7432] [data-file]");
    eprintln!("  postemberdb check [data-file]");
    eprintln!("  postemberdb checkpoint [data-file]");
    eprintln!("  postemberdb version");
    eprintln!("  postemberdb help");
}
