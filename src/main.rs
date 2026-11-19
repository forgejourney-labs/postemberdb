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
        Some("migrate-pdb6") => {
            let data_path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
            let mut database = Database::open(&data_path)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            database.migrate_to_pdb6()
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            println!("PDB6 is now the SQL authority for {data_path}; PDB5 files were retained.");
            Ok(())
        }
        Some("server-tls") => {
            let address = args.next().unwrap_or_else(|| "0.0.0.0:7432".into());
            let data_path = args.next().unwrap_or_else(|| "postemberdb.pbd".into());
            let cert_path = args.next().unwrap_or_else(|| "server-cert.pem".into());
            let key_path = args.next().unwrap_or_else(|| "server-key.pem".into());
            server::run_server_tls(&address, &data_path, &cert_path, &key_path)
        }
        Some("user") => run_user_command(args.collect()),
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

fn run_user_command(args: Vec<String>) -> std::io::Result<()> {
    if args.len() != 4 || args[0] != "add" {
        eprintln!("Usage: postemberdb user add <data-file> <username> <admin|read-only|read-write>");
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid user command"));
    }
    let data_path = &args[1];
    let username = &args[2];
    let permissions = match args[3].as_str() {
        "admin" => postemberdb::Permissions::ADMIN,
        "read-only" => postemberdb::Permissions::READ_ONLY,
        "read-write" => postemberdb::Permissions::READ_WRITE,
        _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "role must be admin, read-only or read-write")),
    };
    let password = rpassword::prompt_password("New user password: ")?;
    let confirmation = rpassword::prompt_password("Confirm password: ")?;
    if password != confirmation {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "passwords do not match"));
    }
    let mut auth = postemberdb::AuthStore::open(format!("{data_path}.auth"))
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    auth.create_user(username, &password, permissions)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    println!("User '{}' created with role '{}'.", username, args[3]);
    Ok(())
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
    eprintln!("  postemberdb server-tls [0.0.0.0:7432] [data-file] [cert.pem] [key.pem]");
    eprintln!("  postemberdb migrate-pdb6 [data-file]");
    eprintln!("  postemberdb user add <data-file> <username> <admin|read-only|read-write>");
    eprintln!("  postemberdb check [data-file]");
    eprintln!("  postemberdb checkpoint [data-file]");
    eprintln!("  postemberdb version");
    eprintln!("  postemberdb help");
}
