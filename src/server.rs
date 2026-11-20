use postemberdb::{AuthStore, Database, DatabaseSession, EmberForge7Namespace, EmberForge7NamespaceKind, EmberForge7PlayerProfile, EmberForge7Store, KeyValueStore, Permission, Permissions, Principal, QueryResult, Registry, RemoteServerStatus, RemoteTablePage, TransactionCoordinator, Value};
use postemberdb::protocol::{Frame, FrameKind, DEFAULT_MAX_PAYLOAD};
use postemberdb::version::version_string;
use std::ffi::OsString;
use std::io::{self, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use serde_json::{json, Value as JsonValue};
use rustls::{pki_types::PrivateKeyDer, ServerConfig, ServerConnection, StreamOwned};
use std::fs::File;

const MAX_CLIENTS: usize = 128;
const MAX_SQL_LINE_BYTES: usize = 64 * 1024;
const CLIENT_IO_TIMEOUT_SECS: u64 = 30;
const MAX_EF7_WIRE_CUSTOM_BYTES: usize = 24 * 1024;
const MAX_EF7_WIRE_CHUNK_BYTES: usize = 24 * 1024;
const MAX_EF7_WIRE_RECORD_BYTES: usize = 24 * 1024;
const MAX_EF7_NAMESPACE_LIST: usize = 64;
const MAX_BINARY_CHUNK_BYTES: usize = DEFAULT_MAX_PAYLOAD - 16;

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

fn validate_bind_address(address: &str, tls_enabled: bool) -> io::Result<SocketAddr> {
    let socket_address: SocketAddr = address.parse().map_err(|_| io::Error::new(
        io::ErrorKind::InvalidInput,
        "server address must be an explicit socket address",
    ))?;
    if !socket_address.ip().is_loopback() && !tls_enabled {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "refusing non-loopback bind: remote connections require TLS and authentication",
        ));
    }
    Ok(socket_address)
}

fn load_tls_config(cert_path: &str, key_path: &str) -> io::Result<Arc<ServerConfig>> {
    let mut cert_reader = BufReader::new(File::open(cert_path)?);
    let certificates = rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "TLS certificate file contains no certificates"));
    }
    let mut key_reader = BufReader::new(File::open(key_path)?);
    let private_key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS private key file contains no supported key"))?;
    let config = ServerConfig::builder().with_no_client_auth().with_single_cert(certificates, private_key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("invalid TLS identity: {error}")))?;
    Ok(Arc::new(config))
}

pub(crate) fn run_server(address: &str, data_path: &str) -> io::Result<()> {
    run_server_inner(address, data_path, None)
}

pub(crate) fn run_server_tls(address: &str, data_path: &str, cert_path: &str, key_path: &str) -> io::Result<()> {
    run_server_inner(address, data_path, Some(load_tls_config(cert_path, key_path)?))
}

fn append_sidecar_suffix(path: &str, suffix: &str) -> PathBuf {
    let mut value = OsString::from(path);
    value.push(suffix);
    PathBuf::from(value)
}

fn open_recovered_stores(
    data_path: &str,
) -> io::Result<(Database, KeyValueStore, Registry, EmberForge7Store, TransactionCoordinator)> {
    let mut database = Database::open(data_path)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut kv = KeyValueStore::open(append_sidecar_suffix(data_path, ".kv"))
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut registry = Registry::open(append_sidecar_suffix(data_path, ".registry"))
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut ef7 = EmberForge7Store::open(data_path, 256)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut coordinator = TransactionCoordinator::open(append_sidecar_suffix(data_path, ".coordinator"))
        .map_err(|error| io::Error::other(error.to_string()))?;

    {
        let mut participants: [&mut dyn postemberdb::CoordinatorParticipant; 4] = [
            &mut database, &mut kv, &mut registry, &mut ef7,
        ];
        coordinator.recover_pending(&mut participants)
            .map_err(|error| io::Error::other(format!("coordinator recovery failed: {error}")))?;
    }
    let in_doubt = coordinator.in_doubt();
    if !in_doubt.is_empty() {
        let ids = in_doubt.iter().map(|transaction| transaction.transaction_id)
            .collect::<Vec<_>>();
        return Err(io::Error::other(format!(
            "server startup blocked: coordinator transactions remain in-doubt (no durable COMMIT/ABORT decision): {ids:?}"
        )));
    }
    Ok((database, kv, registry, ef7, coordinator))
}

fn run_server_inner(address: &str, data_path: &str, tls_config: Option<Arc<ServerConfig>>) -> io::Result<()> {
    let tls_enabled = tls_config.is_some();
    let socket_address = validate_bind_address(address, tls_enabled)?;

    // Recover durable cross-domain decisions before accepting any clients.
    let (database_handle, kv_store, registry_store, ef7_handle, coordinator) =
        open_recovered_stores(data_path)?;
    let database = Arc::new(std::sync::Mutex::new(database_handle));
    let ef7_store = Arc::new(std::sync::Mutex::new(ef7_handle));
    // Keep all participant handles and the coordinator lock alive for the
    // server lifetime so a second process cannot race recovery or open them.
    let _kv_store = kv_store;
    let _registry_store = registry_store;
    let _coordinator = coordinator;

    let listener = TcpListener::bind(socket_address)?;
    if tls_enabled {
        println!("{} — TLS server listening on {socket_address}; authentication required", version_string());
    } else {
        println!("{} — development server listening on loopback {socket_address}", version_string());
    }
    let auth_store = if tls_enabled {
        Some(Arc::new(std::sync::Mutex::new(
            AuthStore::open(format!("{data_path}.auth")).map_err(|error| io::Error::other(error.to_string()))?
        )))
    } else { None };
    let active_clients = Arc::new(AtomicUsize::new(0));
    let started_at = Instant::now();

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                if let Err(error) = stream.set_nodelay(true)
                    .and_then(|_| stream.set_read_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS))))
                    .and_then(|_| stream.set_write_timeout(Some(std::time::Duration::from_secs(CLIENT_IO_TIMEOUT_SECS)))) {
                    eprintln!("connection setup failed: {error}");
                    continue;
                }
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
                            let auth = auth_store.clone();
                            let tls = tls_config.clone();
                            let active = active_clients.clone();
                            let active_for_handler = active_clients.clone();
                            let active_on_failure = active_clients.clone();
                            match thread::Builder::new().name("postemberdb-client".into()).spawn(move || {
                                let _guard = ConnectionGuard(active);
                                let result = if let Some(config) = tls {
                                    match ServerConnection::new(config) {
                                        Ok(connection) => handle_client(
                                            StreamOwned::new(connection, stream), db, ef7, auth, true,
                                            active_for_handler, started_at,
                                        ),
                                        Err(error) => Err(io::Error::new(io::ErrorKind::Other, error.to_string())),
                                    }
                                } else {
                                    handle_client(stream, db, ef7, auth, false, active_for_handler, started_at)
                                };
                                if let Err(error) = result { eprintln!("client error: {error}"); }
                            }) {
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

fn handle_client<S: Read + Write>(
    stream: S,
    database: Arc<std::sync::Mutex<Database>>,
    ef7: Arc<std::sync::Mutex<EmberForge7Store>>,
    auth: Option<Arc<std::sync::Mutex<AuthStore>>>,
    auth_required: bool,
    active_clients: Arc<AtomicUsize>,
    started_at: Instant,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::with_capacity(1024);
    let mut session: Option<DatabaseSession> = None;
    let mut jsonl_mode = false;
    let mut principal: Option<Principal> = None;
    writeln!(reader.get_mut(), "PostemberDB/0.1 ready; {}; send one SQL statement per line", version_string())?;
    loop {
        let sql = match read_bounded_line(&mut reader, &mut line, MAX_SQL_LINE_BYTES) {
            Ok(Some(_)) => std::str::from_utf8(&line).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "SQL input must be valid UTF-8"))?.to_owned(),
            Ok(None) => break,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                writeln!(reader.get_mut(), "ERROR {error}")?;
                break;
            }
            Err(error) => return Err(error),
        };
        let command = sql.trim();
        if command.eq_ignore_ascii_case("QUIT") { break; }
        if command.eq_ignore_ascii_case("POSTEMBERDB BINARY/1") {
            if auth_required && principal.is_none() {
                writeln!(reader.get_mut(), "ERROR authentication required; authenticate before switching protocols")?;
                continue;
            }
            Frame::new(FrameKind::Pong, 0, b"postemberdb-binary/1-ready".to_vec())
                .write_to(reader.get_mut(), DEFAULT_MAX_PAYLOAD)?;
            return handle_binary_client(
                &mut reader, database, ef7, principal, auth_required,
                auth.clone(), active_clients.clone(), started_at, auth_required,
            );
        }
        if command.eq_ignore_ascii_case("POSTEMBERDB JSONL/1") {
            jsonl_mode = true;
            reader.get_mut().write_all(b"{\"protocol\":\"postemberdb-jsonl\",\"version\":1,\"ready\":true}\n")?;
            reader.get_mut().flush()?;
            continue;
        }
        if command.get(..5).is_some_and(|prefix| prefix.eq_ignore_ascii_case("AUTH ")) {
            let response = match command[5..].split_once(' ') {
                Some((username, password)) if !username.is_empty() && !password.is_empty() => match auth.as_ref() {
                    Some(store) => match store.lock() {
                        Ok(mut store) => match store.authenticate(username, password) {
                            Ok(Some(value)) => {
                                principal = Some(value);
                                if jsonl_mode { json_ok_message("authenticated") } else { "OK authenticated\n".to_owned() }
                            }
                            Ok(None) => if jsonl_mode { json_error("authentication failed") } else { "ERROR authentication failed\n".to_owned() },
                            Err(error) => if jsonl_mode { json_error(&error.to_string()) } else { format!("ERROR {error}\n") },
                        },
                        Err(_) => if jsonl_mode { json_error("authentication service unavailable") } else { "ERROR authentication service unavailable\n".to_owned() },
                    },
                    None => if jsonl_mode { json_error("authentication is not enabled on this listener") } else { "ERROR authentication is not enabled\n".to_owned() },
                },
                _ => if jsonl_mode { json_error("AUTH requires username and password") } else { "ERROR AUTH requires username and password\n".to_owned() },
            };
            reader.get_mut().write_all(response.as_bytes())?;
            reader.get_mut().flush()?;
            continue;
        }
        if auth_required && principal.is_none() {
            let response = if jsonl_mode { json_error("authentication required") } else { "ERROR authentication required\n".to_owned() };
            reader.get_mut().write_all(response.as_bytes())?;
            reader.get_mut().flush()?;
            continue;
        }
        if jsonl_mode && command.starts_with("EF7 ") {
            let permission_error = if auth_required {
                principal.as_ref().and_then(|p| permission_for_ef7(command).and_then(|permission| p.require(permission).err()))
            } else { None };
            let response = if let Some(error) = permission_error {
                json_error(&error.to_string())
            } else if auth_required && permission_for_ef7(command).is_none() {
                json_error("EF7 command is not covered by the RBAC policy")
            } else if session.is_some() {
                json_error("EF7 commands cannot run inside a SQL transaction; EF7 is a separate storage domain")
            } else {
                match ef7.lock() {
                    Ok(mut store) => handle_ef7_command(command, &mut store),
                    Err(_) => json_error("EF7 storage lock poisoned"),
                }
            };
            reader.get_mut().write_all(response.as_bytes())?;
            reader.get_mut().flush()?;
            continue;
        }
        let permission_error = if auth_required {
            principal.as_ref().and_then(|p| permission_for_sql(command).and_then(|permission| p.require(permission).err()))
        } else { None };
        if let Some(error) = permission_error {
            let response = if jsonl_mode { json_error(&error.to_string()) } else { format!("ERROR {error}\n") };
            reader.get_mut().write_all(response.as_bytes())?;
            reader.get_mut().flush()?;
            continue;
        }
        if auth_required && permission_for_sql(command).is_none() {
            let response = if jsonl_mode { json_error("SQL command is not covered by the RBAC policy") } else { "ERROR SQL command is not covered by the RBAC policy\n".to_owned() };
            reader.get_mut().write_all(response.as_bytes())?;
            reader.get_mut().flush()?;
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
        reader.get_mut().write_all(response.as_bytes())?;
        reader.get_mut().flush()?;
    }
    Ok(())
}

fn permission_for_sql(command: &str) -> Option<Permission> {
    let verb = command.split_whitespace().next()?.trim_matches(|c: char| !c.is_ascii_alphabetic()).to_ascii_uppercase();
    match verb.as_str() {
        "SELECT" | "SHOW" | "DESCRIBE" | "EXPLAIN" => Some(Permission::SqlRead),
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "ALTER" | "DROP" | "TRUNCATE"
        | "REPLACE" | "BEGIN" | "COMMIT" | "ROLLBACK" => Some(Permission::SqlWrite),
        _ => None,
    }
}

fn permission_for_ef7(command: &str) -> Option<Permission> {
    let operation = command.split_whitespace().nth(1)?.to_ascii_uppercase();
    match operation.as_str() {
        "NS_GET" | "NS_FIND" | "NS_LIST" | "CHUNK_GET" | "RECORD_GET"
        | "PROFILE_GET" | "PROFILE_GET_NAME" => Some(Permission::Ef7Read),
        "NS_CREATE" | "NS_DELETE" | "CHUNK_PUT" | "CHUNK_DELETE"
        | "RECORD_PUT" | "RECORD_PUT_TTL" | "RECORD_DELETE" | "RECORD_SWEEP"
        | "PROFILE_PUT" | "PROFILE_BATCH" | "PLAYER_DELETE" => Some(Permission::Ef7Write),
        _ => None,
    }
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

fn parse_optional_namespace_parent(value: &str) -> Result<Option<u64>, String> {
    if value == "-" {
        return Ok(None);
    }
    let parent = value.parse::<u64>().map_err(|_| "invalid namespace parent ID".to_owned())?;
    if parent == 0 {
        return Err("namespace parent ID must be non-zero".into());
    }
    Ok(Some(parent))
}

fn parse_namespace_kind(value: &str) -> Result<EmberForge7NamespaceKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "server" => Ok(EmberForge7NamespaceKind::Server),
        "world" => Ok(EmberForge7NamespaceKind::World),
        "plugin" => Ok(EmberForge7NamespaceKind::Plugin),
        "playerdata" | "player_data" => Ok(EmberForge7NamespaceKind::PlayerData),
        "custom" => Ok(EmberForge7NamespaceKind::Custom),
        _ => Err("namespace kind must be server, world, plugin, playerdata or custom".into()),
    }
}

fn namespace_kind_name(kind: EmberForge7NamespaceKind) -> &'static str {
    match kind {
        EmberForge7NamespaceKind::Server => "server",
        EmberForge7NamespaceKind::World => "world",
        EmberForge7NamespaceKind::Plugin => "plugin",
        EmberForge7NamespaceKind::PlayerData => "playerdata",
        EmberForge7NamespaceKind::Custom => "custom",
    }
}

fn json_namespace(namespace: &EmberForge7Namespace) -> String {
    let parent = namespace.parent_id.map_or_else(|| "null".to_owned(), |id| id.to_string());
    format!(
        "{{\"id\":{},\"parent_id\":{},\"kind\":{},\"name\":{}}}",
        namespace.id, parent, json_escape(namespace_kind_name(namespace.kind)), json_escape(&namespace.name)
    )
}

fn json_namespaces(namespaces: &[EmberForge7Namespace]) -> String {
    let encoded = namespaces.iter().map(json_namespace).collect::<Vec<_>>().join(",");
    format!("[{encoded}]")
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

fn binary_error(request_id: u64, message: &str) -> Frame {
    Frame::new(FrameKind::Error, request_id, message.as_bytes().to_vec())
}

fn handle_binary_client<S: Read + Write>(
    reader: &mut BufReader<S>,
    database: Arc<std::sync::Mutex<Database>>,
    ef7: Arc<std::sync::Mutex<EmberForge7Store>>,
    principal: Option<Principal>,
    auth_required: bool,
    auth: Option<Arc<std::sync::Mutex<AuthStore>>>,
    active_clients: Arc<AtomicUsize>,
    started_at: Instant,
    tls_enabled: bool,
) -> io::Result<()> {
    loop {
        let frame = match Frame::read_from_or_eof(reader, DEFAULT_MAX_PAYLOAD)? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let request_id = frame.request_id;
        let response = match frame.kind {
            FrameKind::Ping => Frame::new(FrameKind::Pong, request_id, frame.payload),
            FrameKind::Catalog => {
                let permission = principal.as_ref().map(|value| value.require(Permission::SqlRead))
                    .unwrap_or_else(|| if auth_required { Err(crate::DbError::Constraint("authentication required".into())) } else { Ok(()) });
                match permission {
                    Err(error) => binary_error(request_id, &error.to_string()),
                    Ok(()) => match database.lock() {
                        Ok(db) => match serde_json::to_vec(&db.catalog()) {
                            Ok(payload) => Frame::new(FrameKind::CatalogResult, request_id, payload),
                            Err(error) => binary_error(request_id, &format!("could not encode SQL catalog: {error}")),
                        },
                        Err(_) => binary_error(request_id, "database lock poisoned"),
                    },
                }
            }
            FrameKind::TablePage => {
                let permission = principal.as_ref().map(|value| value.require(Permission::SqlRead))
                    .unwrap_or_else(|| if auth_required { Err(crate::DbError::Constraint("authentication required".into())) } else { Ok(()) });
                if let Err(error) = permission {
                    binary_error(request_id, &error.to_string())
                } else {
                    match serde_json::from_slice::<TablePageRequest>(&frame.payload) {
                        Err(error) => binary_error(request_id, &format!("invalid table-page request: {error}")),
                        Ok(request) if request.table.is_empty() || request.table.len() > 128
                            || request.limit == 0 || request.limit > 1_000 =>
                            binary_error(request_id, "table-page request requires a table name and limit 1..=1000"),
                        Ok(request) => match database.lock() {
                            Ok(db) => {
                                let result = db.table_info(&request.table).and_then(|info| {
                                    let page = db.read_table_page(&info.name, request.offset, request.limit)?;
                                    Ok((info.row_count, info.name, page))
                                });
                                match result {
                                    Ok((total_rows, table, result)) => {
                                        let response = RemoteTablePage {
                                            table, offset: request.offset, limit: request.limit,
                                            total_rows, result,
                                        };
                                        match serde_json::to_vec(&response) {
                                            Ok(payload) => Frame::new(FrameKind::TablePageResult, request_id, payload),
                                            Err(error) => binary_error(request_id, &format!("could not encode table page: {error}")),
                                        }
                                    }
                                    Err(error) => binary_error(request_id, &error.to_string()),
                                }
                            }
                            Err(_) => binary_error(request_id, "database lock poisoned"),
                        },
                    }
                }
            }
            FrameKind::BatchQuery => {
                let permission = principal.as_ref().map(|value| value.require(Permission::SqlWrite))
                    .unwrap_or_else(|| if auth_required { Err(crate::DbError::Constraint("authentication required".into())) } else { Ok(()) });
                if let Err(error) = permission {
                    binary_error(request_id, &error.to_string())
                } else {
                    match serde_json::from_slice::<BatchQueryRequest>(&frame.payload) {
                        Err(error) => binary_error(request_id, &format!("invalid transaction batch: {error}")),
                        Ok(request) if request.statements.is_empty() || request.statements.len() > 500
                            || request.statements.iter().any(|statement| statement.trim().is_empty()
                                || permission_for_sql(statement.trim()) != Some(Permission::SqlWrite)) =>
                            binary_error(request_id, "transaction batch must contain 1..=500 SQL write statements"),
                        Ok(request) => match database.lock() {
                            Ok(mut db) => match db.transaction(|transaction| {
                                for statement in &request.statements {
                                    transaction.execute(statement)?;
                                }
                                Ok(())
                            }) {
                                Ok(()) => Frame::new(
                                    FrameKind::BatchQueryResult, request_id,
                                    json!({"ok":true,"message":"transaction batch committed","statements":request.statements.len()}).to_string().into_bytes(),
                                ),
                                Err(error) => binary_error(request_id, &error.to_string()),
                            },
                            Err(_) => binary_error(request_id, "database lock poisoned"),
                        },
                    }
                }
            }
            FrameKind::ServerStatus => {
                if auth_required && principal.as_ref().is_none_or(|value| !value.allows(Permission::Admin)) {
                    binary_error(request_id, "server status requires the Admin permission")
                } else {
                    let status = RemoteServerStatus {
                        version: version_string(),
                        active_clients: active_clients.load(Ordering::Acquire),
                        maximum_clients: MAX_CLIENTS,
                        tls_enabled,
                        uptime_seconds: started_at.elapsed().as_secs(),
                    };
                    match serde_json::to_vec(&status) {
                        Ok(payload) => Frame::new(FrameKind::ServerStatusResult, request_id, payload),
                        Err(error) => binary_error(request_id, &format!("could not encode server status: {error}")),
                    }
                }
            }
            FrameKind::AdminRequest => {
                match handle_binary_admin_request(&frame.payload, auth.as_ref(), principal.as_ref()) {
                    Ok(payload) => Frame::new(FrameKind::AdminResponse, request_id, payload),
                    Err(error) => binary_error(request_id, &error),
                }
            }
            FrameKind::Query => {
                let sql = match std::str::from_utf8(&frame.payload) {
                    Ok(sql) if !sql.trim().is_empty() => sql,
                    _ => {
                        binary_error(request_id, "query payload must be non-empty UTF-8 SQL")
                            .write_to(reader.get_mut(), DEFAULT_MAX_PAYLOAD)?;
                        continue;
                    }
                };
                let command = sql.trim();
                let permission_error = if auth_required {
                    principal.as_ref().and_then(|p| permission_for_sql(command).and_then(|permission| p.require(permission).err()))
                } else { None };
                if auth_required && permission_for_sql(command).is_none() {
                    binary_error(request_id, "SQL command is not covered by the RBAC policy")
                } else if let Some(error) = permission_error {
                    binary_error(request_id, &error.to_string())
                } else if ["BEGIN", "COMMIT", "ROLLBACK"].iter().any(|word| command.eq_ignore_ascii_case(word)) {
                    binary_error(request_id, "binary/1 query frames do not support transaction control yet")
                } else {
                    match database.lock() {
                        Ok(mut db) => match db.execute(sql) {
                            Ok(result) => Frame::new(FrameKind::QueryResult, request_id, format_result_json(result).into_bytes()),
                            Err(error) => binary_error(request_id, &error.to_string()),
                        },
                        Err(_) => binary_error(request_id, "database lock poisoned"),
                    }
                }
            }
            FrameKind::ChunkPut => {
                if auth_required && principal.as_ref().is_some_and(|p| !p.allows(Permission::Ef7Write)) {
                    binary_error(request_id, "not authorized for EF7 writes")
                } else if frame.payload.len() < 16 {
                    binary_error(request_id, "chunk put payload requires namespace:u64, x:i32, z:i32 and bytes")
                } else if frame.payload.len() - 16 > MAX_BINARY_CHUNK_BYTES {
                    binary_error(request_id, "chunk payload exceeds binary/1 single-frame limit")
                } else {
                    let namespace = u64::from_le_bytes(frame.payload[0..8].try_into().unwrap());
                    let x = i32::from_le_bytes(frame.payload[8..12].try_into().unwrap());
                    let z = i32::from_le_bytes(frame.payload[12..16].try_into().unwrap());
                    match ef7.lock() {
                        Ok(mut store) => match store.put_chunk(namespace, x, z, &frame.payload[16..]) {
                            Ok(()) => Frame::new(FrameKind::QueryResult, request_id, b"{\"ok\":true,\"message\":\"chunk stored\"}".to_vec()),
                            Err(error) => binary_error(request_id, &error.to_string()),
                        },
                        Err(_) => binary_error(request_id, "EF7 storage lock poisoned"),
                    }
                }
            }
            FrameKind::ChunkGet => {
                if auth_required && principal.as_ref().is_some_and(|p| !p.allows(Permission::Ef7Read)) {
                    binary_error(request_id, "not authorized for EF7 reads")
                } else if frame.payload.len() != 16 {
                    binary_error(request_id, "chunk get payload must contain namespace:u64, x:i32 and z:i32")
                } else {
                    let namespace = u64::from_le_bytes(frame.payload[0..8].try_into().unwrap());
                    let x = i32::from_le_bytes(frame.payload[8..12].try_into().unwrap());
                    let z = i32::from_le_bytes(frame.payload[12..16].try_into().unwrap());
                    match ef7.lock() {
                        Ok(mut store) => match store.get_chunk_limited(namespace, x, z, MAX_BINARY_CHUNK_BYTES) {
                            Ok(Some(payload)) => {
                                let mut response = Vec::with_capacity(16 + payload.len());
                                response.extend_from_slice(&namespace.to_le_bytes());
                                response.extend_from_slice(&x.to_le_bytes());
                                response.extend_from_slice(&z.to_le_bytes());
                                response.extend_from_slice(&payload);
                                Frame::new(FrameKind::ChunkData, request_id, response)
                            }
                            Ok(None) => Frame::new(FrameKind::ChunkData, request_id, Vec::new()),
                            Err(error) => binary_error(request_id, &error.to_string()),
                        },
                        Err(_) => binary_error(request_id, "EF7 storage lock poisoned"),
                    }
                }
            }
            FrameKind::ChunkDelete => {
                if auth_required && principal.as_ref().is_some_and(|p| !p.allows(Permission::Ef7Write)) {
                    binary_error(request_id, "not authorized for EF7 writes")
                } else if frame.payload.len() != 16 {
                    binary_error(request_id, "chunk delete payload must contain namespace:u64, x:i32 and z:i32")
                } else {
                    let namespace = u64::from_le_bytes(frame.payload[0..8].try_into().unwrap());
                    let x = i32::from_le_bytes(frame.payload[8..12].try_into().unwrap());
                    let z = i32::from_le_bytes(frame.payload[12..16].try_into().unwrap());
                    match ef7.lock() {
                        Ok(mut store) => match store.delete_chunk(namespace, x, z) {
                            Ok(deleted) => Frame::new(
                                FrameKind::QueryResult, request_id,
                                format!("{{\"ok\":true,\"affected_rows\":{}}}", u8::from(deleted)).into_bytes()
                            ),
                            Err(error) => binary_error(request_id, &error.to_string()),
                        },
                        Err(_) => binary_error(request_id, "EF7 storage lock poisoned"),
                    }
                }
            }
            FrameKind::CatalogResult | FrameKind::TablePageResult | FrameKind::ServerStatusResult
            | FrameKind::AdminResponse | FrameKind::BatchQueryResult
            | FrameKind::QueryResult | FrameKind::Error | FrameKind::Pong
            | FrameKind::ChunkData | FrameKind::ChunkScan | FrameKind::ChunkScanResult => {
                binary_error(request_id, "frame kind is response-only or not implemented by this server version")
            }
        };
        response.write_to(reader.get_mut(), DEFAULT_MAX_PAYLOAD)?;
    }
}


#[derive(serde::Deserialize)]
struct TablePageRequest {
    table: String,
    offset: usize,
    limit: usize,
}

#[derive(serde::Deserialize)]
struct BatchQueryRequest {
    statements: Vec<String>,
}

fn handle_binary_admin_request(
    payload: &[u8],
    auth: Option<&Arc<std::sync::Mutex<AuthStore>>>,
    principal: Option<&Principal>,
) -> Result<Vec<u8>, String> {
    let principal = principal.ok_or_else(|| "administrative operations require an authenticated administrator".to_owned())?;
    principal.require(Permission::Admin).map_err(|error| error.to_string())?;
    let auth = auth.ok_or_else(|| "authentication store is unavailable on this listener".to_owned())?;
    let request: JsonValue = serde_json::from_slice(payload)
        .map_err(|error| format!("invalid administrative request: {error}"))?;
    let action = request.get("action").and_then(JsonValue::as_str)
        .ok_or_else(|| "administrative request requires an action".to_owned())?;
    let mut store = auth.lock().map_err(|_| "authentication store lock poisoned".to_owned())?;
    let result = store.as_admin(principal, |store| {
        let text = |field: &str| -> Result<String, crate::DbError> {
            request.get(field).and_then(JsonValue::as_str).map(str::to_owned)
                .ok_or_else(|| crate::DbError::Constraint(format!("administrative request requires text field '{field}'")))
        };
        let number = |field: &str| -> Result<u64, crate::DbError> {
            request.get(field).and_then(JsonValue::as_u64)
                .ok_or_else(|| crate::DbError::Constraint(format!("administrative request requires integer field '{field}'")))
        };
        let permission_set = || -> Result<Permissions, crate::DbError> {
            let bits = u32::try_from(number("permissions")?)
                .map_err(|_| crate::DbError::Constraint("permissions bitset is out of range".into()))?;
            Permissions::from_bits(bits)
        };
        match action {
            "users.list" => Ok(json!({"users": store.list_user_details()?})),
            "users.create" => {
                let username = text("username")?;
                let password = text("password")?;
                store.create_user(&username, &password, permission_set()?)?;
                Ok(json!({"created": true, "username": username}))
            }
            "users.permissions" => {
                let username = text("username")?;
                if username.eq_ignore_ascii_case(&principal.username) {
                    return Err(crate::DbError::Constraint("an administrator may not change their own direct permissions through this session".into()));
                }
                store.set_permissions(&username, permission_set()?)?;
                Ok(json!({"updated": true, "username": username}))
            }
            "users.enabled" => {
                let username = text("username")?;
                let enabled = request.get("enabled").and_then(JsonValue::as_bool)
                    .ok_or_else(|| crate::DbError::Constraint("administrative request requires boolean field 'enabled'".into()))?;
                if username.eq_ignore_ascii_case(&principal.username) && !enabled {
                    return Err(crate::DbError::Constraint("an administrator may not disable their own account through this session".into()));
                }
                store.set_enabled(&username, enabled)?;
                Ok(json!({"updated": true, "username": username, "enabled": enabled}))
            }
            "users.delete" => {
                let username = text("username")?;
                if username.eq_ignore_ascii_case(&principal.username) {
                    return Err(crate::DbError::Constraint("an administrator may not delete their own account through this session".into()));
                }
                let deleted = store.delete_user(&username)?;
                Ok(json!({"deleted": deleted, "username": username}))
            }
            "roles.list" => Ok(json!({"roles": store.list_role_details()?})),
            "roles.create" => {
                let name = text("name")?;
                store.create_role(&name, permission_set()?)?;
                Ok(json!({"created": true, "name": name}))
            }
            "roles.permissions" => {
                let name = text("name")?;
                store.set_role_permissions(&name, permission_set()?)?;
                Ok(json!({"updated": true, "name": name}))
            }
            "roles.assign" => {
                let username = text("username")?;
                let role = text("role")?;
                store.assign_role(&username, &role)?;
                Ok(json!({"assigned": true, "username": username, "role": role}))
            }
            "roles.revoke" => {
                let username = text("username")?;
                let role = text("role")?;
                let revoked = store.revoke_role(&username, &role)?;
                Ok(json!({"revoked": revoked, "username": username, "role": role}))
            }
            "roles.delete" => {
                let name = text("name")?;
                let deleted = store.delete_role(&name)?;
                Ok(json!({"deleted": deleted, "name": name}))
            }
            "audit.list" => {
                let after = number("after_sequence").unwrap_or(0);
                let limit = request.get("limit").and_then(JsonValue::as_u64).unwrap_or(100);
                let limit = usize::try_from(limit)
                    .map_err(|_| crate::DbError::Constraint("audit limit is out of range".into()))?;
                if limit > 500 {
                    return Err(crate::DbError::Constraint("remote audit page is limited to 500 events".into()));
                }
                Ok(json!({"events": store.audit_events(after, limit)?}))
            }
            _ => Err(crate::DbError::Unsupported(format!("unknown administrative action: {action}"))),
        }
    }).map_err(|error| error.to_string())?;
    serde_json::to_vec(&json!({"ok":true,"result":result}))
        .map_err(|error| format!("could not encode administrative response: {error}"))
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
    let parse_i32 = |index: usize, label: &str| -> Result<i32, String> {
        parts.get(index).ok_or_else(|| format!("missing {label}"))?
            .parse::<i32>().map_err(|_| format!("invalid {label}"))
    };
    let result: Result<String, String> = (|| {
        match parts[1] {
            "NS_CREATE" => {
                if parts.len() != 6 { return Err("usage: EF7 NS_CREATE <id> <parent_id|-> <kind> <name>".into()); }
                let id = parse_id(2, "namespace ID")?;
                let parent_id = parse_optional_namespace_parent(parts[3])?;
                let kind = parse_namespace_kind(parts[4])?;
                let namespace = store.create_namespace(id, parent_id, kind, parts[5])
                    .map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 namespace created\",\"namespace\":{}}}\n",
                    json_namespace(&namespace)
                ))
            }
            "NS_GET" => {
                if parts.len() != 3 { return Err("usage: EF7 NS_GET <id>".into()); }
                let id = parse_id(2, "namespace ID")?;
                let namespace = store.get_namespace(id).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 namespace lookup\",\"namespace\":{}}}\n",
                    namespace.as_ref().map_or_else(|| "null".to_owned(), json_namespace)
                ))
            }
            "NS_FIND" => {
                if parts.len() != 4 { return Err("usage: EF7 NS_FIND <parent_id|-> <name>".into()); }
                let parent_id = parse_optional_namespace_parent(parts[2])?;
                let namespace = store.find_namespace(parent_id, parts[3]).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 namespace lookup\",\"namespace\":{}}}\n",
                    namespace.as_ref().map_or_else(|| "null".to_owned(), json_namespace)
                ))
            }
            "NS_LIST" => {
                if parts.len() != 4 { return Err("usage: EF7 NS_LIST <parent_id|-> <limit>".into()); }
                let parent_id = parse_optional_namespace_parent(parts[2])?;
                let limit = parts[3].parse::<usize>().map_err(|_| "invalid namespace list limit".to_owned())?;
                if limit == 0 || limit > MAX_EF7_NAMESPACE_LIST {
                    return Err(format!("KSL namespace list limit must be 1..={MAX_EF7_NAMESPACE_LIST}"));
                }
                let namespaces = store.list_namespaces(parent_id, limit).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 namespaces listed\",\"namespaces\":{}}}\n",
                    json_namespaces(&namespaces)
                ))
            }
            "NS_DELETE" => {
                if parts.len() != 3 { return Err("usage: EF7 NS_DELETE <id>".into()); }
                let id = parse_id(2, "namespace ID")?;
                let deleted = store.delete_namespace(id).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":{},\"affected_rows\":{}}}\n",
                    json_escape(if deleted { "EF7 namespace deleted" } else { "EF7 namespace not found" }),
                    u8::from(deleted)
                ))
            }
            "CHUNK_PUT" => {
                if parts.len() != 6 { return Err("usage: EF7 CHUNK_PUT <namespace> <chunk_x> <chunk_z> <payload_hex|->".into()); }
                let namespace = parse_id(2, "world namespace ID")?;
                let chunk_x = parse_i32(3, "chunk X")?;
                let chunk_z = parse_i32(4, "chunk Z")?;
                let payload = if parts[5] == "-" { Vec::new() } else { decode_hex(parts[5])? };
                if payload.len() > MAX_EF7_WIRE_CHUNK_BYTES {
                    return Err(format!("chunk payload exceeds the KSL JSONL/1 limit of {MAX_EF7_WIRE_CHUNK_BYTES} bytes"));
                }
                store.put_chunk(namespace, chunk_x, chunk_z, &payload).map_err(|error| error.to_string())?;
                Ok(json_ok_message("EF7 chunk stored"))
            }
            "CHUNK_GET" => {
                if parts.len() != 5 { return Err("usage: EF7 CHUNK_GET <namespace> <chunk_x> <chunk_z>".into()); }
                let namespace = parse_id(2, "world namespace ID")?;
                let chunk_x = parse_i32(3, "chunk X")?;
                let chunk_z = parse_i32(4, "chunk Z")?;
                match store.get_chunk_limited(namespace, chunk_x, chunk_z, MAX_EF7_WIRE_CHUNK_BYTES)
                    .map_err(|error| error.to_string())? {
                    Some(payload) => Ok(format!(
                        "{{\"ok\":true,\"message\":\"EF7 chunk found\",\"chunk\":{{\"namespace_id\":{},\"chunk_x\":{},\"chunk_z\":{},\"payload_hex\":{}}}}}\n",
                        namespace, chunk_x, chunk_z, json_escape(&encode_hex(&payload))
                    )),
                    None => Ok("{\"ok\":true,\"message\":\"EF7 chunk not found\",\"chunk\":null}\n".into()),
                }
            }
            "CHUNK_DELETE" => {
                if parts.len() != 5 { return Err("usage: EF7 CHUNK_DELETE <namespace> <chunk_x> <chunk_z>".into()); }
                let namespace = parse_id(2, "world namespace ID")?;
                let chunk_x = parse_i32(3, "chunk X")?;
                let chunk_z = parse_i32(4, "chunk Z")?;
                let deleted = store.delete_chunk(namespace, chunk_x, chunk_z).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":{},\"affected_rows\":{}}}\n",
                    json_escape(if deleted { "EF7 chunk deleted" } else { "EF7 chunk not found" }),
                    u8::from(deleted)
                ))
            }
            "RECORD_PUT" => {
                if parts.len() != 5 { return Err("usage: EF7 RECORD_PUT <namespace> <record_id> <payload_hex|->".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let record_id = parse_id(3, "record ID")?;
                let payload = if parts[4] == "-" { Vec::new() } else { decode_hex(parts[4])? };
                if payload.len() > MAX_EF7_WIRE_RECORD_BYTES {
                    return Err(format!("record payload exceeds the JSONL/1 bridge limit of {MAX_EF7_WIRE_RECORD_BYTES} bytes"));
                }
                store.put_record(namespace, record_id, &payload).map_err(|error| error.to_string())?;
                Ok(json_ok_message("EF7 record stored"))
            }
            "RECORD_PUT_TTL" => {
                if parts.len() != 6 { return Err("usage: EF7 RECORD_PUT_TTL <namespace> <record_id> <expires_at_ms> <payload_hex|->".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let record_id = parse_id(3, "record ID")?;
                let expires_at_ms = parse_id(4, "expires_at_ms")?;
                let payload = if parts[5] == "-" { Vec::new() } else { decode_hex(parts[5])? };
                if payload.len() > MAX_EF7_WIRE_RECORD_BYTES {
                    return Err(format!("record payload exceeds the JSONL/1 bridge limit of {MAX_EF7_WIRE_RECORD_BYTES} bytes"));
                }
                store.put_record_with_expiry(namespace, record_id, expires_at_ms, &payload)
                    .map_err(|error| error.to_string())?;
                Ok(json_ok_message("EF7 TTL record stored"))
            }
            "RECORD_GET" => {
                if parts.len() != 4 { return Err("usage: EF7 RECORD_GET <namespace> <record_id>".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let record_id = parse_id(3, "record ID")?;
                match store.get_record_limited(namespace, record_id, MAX_EF7_WIRE_RECORD_BYTES)
                    .map_err(|error| error.to_string())? {
                    Some(payload) => Ok(format!(
                        "{{\"ok\":true,\"message\":\"EF7 record found\",\"record\":{{\"namespace_id\":{},\"record_id\":{},\"payload_hex\":{}}}}}\n",
                        namespace, record_id, json_escape(&encode_hex(&payload))
                    )),
                    None => Ok("{\"ok\":true,\"message\":\"EF7 record not found\",\"record\":null}\n".into()),
                }
            }
            "RECORD_DELETE" => {
                if parts.len() != 4 { return Err("usage: EF7 RECORD_DELETE <namespace> <record_id>".into()); }
                let namespace = parse_id(2, "namespace ID")?;
                let record_id = parse_id(3, "record ID")?;
                let deleted = store.delete_record(namespace, record_id).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":{},\"affected_rows\":{}}}\n",
                    json_escape(if deleted { "EF7 record deleted" } else { "EF7 record not found" }),
                    u8::from(deleted)
                ))
            }
            "RECORD_SWEEP" => {
                if parts.len() != 4 { return Err("usage: EF7 RECORD_SWEEP <now_ms> <limit>".into()); }
                let now_ms = parse_id(2, "current time in milliseconds")?;
                let limit = parts[3].parse::<usize>().map_err(|_| "invalid TTL sweep limit".to_owned())?;
                let removed = store.sweep_expired_records(now_ms, limit).map_err(|error| error.to_string())?;
                Ok(format!(
                    "{{\"ok\":true,\"message\":\"EF7 TTL sweep completed\",\"affected_rows\":{removed}}}\n"
                ))
            }
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
    use super::{append_sidecar_suffix, open_recovered_stores, read_bounded_line, validate_bind_address};
    use postemberdb::protocol::{Frame, FrameKind, DEFAULT_MAX_PAYLOAD};
    use postemberdb::{Database, EmberForge7NamespaceKind, EmberForge7Store, KeyValueStore, QueryResult, Registry, TransactionCoordinator, Value};
    use std::io::{self, BufRead, BufReader, Cursor, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn server_startup_recovers_committed_sql_kv_registry_and_ef7_participants() {
        let directory = std::env::temp_dir().join(format!(
            "postemberdb-server-recovery-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let data_path = directory.join("server.pbd").to_string_lossy().into_owned();
        let tx;
        {
            let mut database = Database::open(&data_path).unwrap();
            database.migrate_to_pdb6().unwrap();
            database.execute("CREATE TABLE startup_players (uuid TEXT, name TEXT)").unwrap();
            let mut kv = KeyValueStore::open(append_sidecar_suffix(&data_path, ".kv")).unwrap();
            let mut registry = Registry::open(append_sidecar_suffix(&data_path, ".registry")).unwrap();
            registry.create_hive("SYSTEM").unwrap();
            let mut ef7 = EmberForge7Store::open(&data_path, 16).unwrap();
            ef7.create_namespace(1, None, EmberForge7NamespaceKind::World, "world").unwrap();
            let mut coordinator = TransactionCoordinator::open(append_sidecar_suffix(&data_path, ".coordinator")).unwrap();

            tx = coordinator.begin(4).unwrap();
            database.prepare_sql(tx, &[String::from(
                "INSERT INTO startup_players (uuid, name) VALUES ('startup-uuid', 'Casey')"
            )]).unwrap();
            kv.prepare_batch(tx, &[], &[(b"startup/key".to_vec(), b"kv-value".to_vec())]).unwrap();
            registry.prepare_set_value(
                tx, "SYSTEM", "StartupRecovery", postemberdb::RegistryValue::String("ok".into()),
            ).unwrap();
            ef7.prepare_put_chunk(tx, 1, 2, -3, b"startup-chunk").unwrap();
            for id in [0, 1, 2, 3] { coordinator.mark_prepared(tx, id).unwrap(); }
            coordinator.commit(tx).unwrap();
        }

        {
            let (mut database, mut kv, mut registry, mut ef7, coordinator) =
                open_recovered_stores(&data_path).unwrap();
            assert_eq!(database.execute("SELECT * FROM startup_players").unwrap().rows.len(), 1);
            assert_eq!(kv.get(b"startup/key").unwrap(), Some(b"kv-value".to_vec()));
            assert_eq!(registry.get_value("SYSTEM", "StartupRecovery").unwrap(),
                Some(postemberdb::RegistryValue::String("ok".into())));
            assert_eq!(ef7.get_chunk(1, 2, -3).unwrap(), Some(b"startup-chunk".to_vec()));
            assert!(database.pending_prepared_transactions().unwrap().is_empty());
            assert!(kv.pending_prepared_transactions().is_empty());
            assert!(registry.pending_prepared_transactions().is_empty());
            assert!(ef7.pending_prepared_transactions().is_empty());
            assert!(coordinator.committed_pending_apply().is_empty());
            assert!(coordinator.in_doubt().is_empty());
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

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
            super::handle_client(stream, database, ef7, None, false).unwrap();
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

        writeln!(client, "EF7 NS_CREATE 1 - server hub").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"name\":\"hub\""));
        writeln!(client, "EF7 NS_CREATE 7 1 world survival").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"kind\":\"world\""));
        writeln!(client, "EF7 NS_FIND 1 SURVIVAL").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"id\":7"));
        writeln!(client, "EF7 NS_LIST 1 10").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"namespaces\":["));
        writeln!(client, "EF7 CHUNK_PUT 7 -12 34 00ff102030").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 chunk stored"));
        writeln!(client, "EF7 NS_DELETE 7").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("not empty"));
        writeln!(client, "EF7 CHUNK_GET 7 -12 34").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"payload_hex\":\"00ff102030\""));
        writeln!(client, "EF7 CHUNK_DELETE 7 -12 34").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 chunk deleted"));

        writeln!(client, "EF7 RECORD_PUT_TTL 7 501 500 01020304").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("EF7 TTL record stored"));
        writeln!(client, "EF7 RECORD_GET 7 501").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"payload_hex\":\"01020304\""));
        writeln!(client, "EF7 RECORD_SWEEP 499 10").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"affected_rows\":0"));
        writeln!(client, "EF7 RECORD_SWEEP 500 10").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"affected_rows\":1"));
        writeln!(client, "EF7 RECORD_GET 7 501").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("\"record\":null"));

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
        writeln!(client, "EF7 NS_DELETE 7").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("namespace deleted"));
        writeln!(client, "EF7 NS_DELETE 1").unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.contains("namespace deleted"));

        writeln!(client, "QUIT").unwrap();
        drop(reader);
        drop(client);
        worker.join().unwrap();
        for suffix in [".ef7.lock", ".ef7.pages", ".ef7.wal"] {
            let mut sidecar = ef7_path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(sidecar));
        }
    }

    #[test]
    fn binary_protocol_handshake_query_and_chunk_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let database = Arc::new(Mutex::new(postemberdb::Database::new()));
        let ef7_path = std::env::temp_dir().join(format!(
            "postemberdb-binary-ef7-{}-{}.pbd", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let ef7 = Arc::new(Mutex::new(EmberForge7Store::open(&ef7_path, 16).unwrap()));
        let worker = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            super::handle_client(stream, database, ef7, None, false).unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        let reader_stream = client.try_clone().unwrap();
        let mut reader = BufReader::new(reader_stream);
        let mut greeting = String::new();
        reader.read_line(&mut greeting).unwrap();
        assert!(greeting.contains("PostemberDB/0.1 ready"));
        writeln!(client, "POSTEMBERDB BINARY/1").unwrap();
        let ready = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(ready.kind, FrameKind::Pong);
        assert_eq!(ready.payload, b"postemberdb-binary/1-ready");

        Frame::new(FrameKind::Ping, 1, b"ping".to_vec())
            .write_to(&mut client, DEFAULT_MAX_PAYLOAD).unwrap();
        let pong = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(pong.kind, FrameKind::Pong);
        assert_eq!(pong.request_id, 1);
        assert_eq!(pong.payload, b"ping");

        Frame::new(FrameKind::Query, 2, b"CREATE TABLE binary_test (id INTEGER)".to_vec())
            .write_to(&mut client, DEFAULT_MAX_PAYLOAD).unwrap();
        let created = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(created.kind, FrameKind::QueryResult);
        assert!(String::from_utf8(created.payload).unwrap().contains("\"ok\":true"));

        let mut put = Vec::new();
        put.extend_from_slice(&1u64.to_le_bytes());
        put.extend_from_slice(&(-20i32).to_le_bytes());
        put.extend_from_slice(&35i32.to_le_bytes());
        put.extend_from_slice(&[0, 1, 0xff, 0x7f]);
        Frame::new(FrameKind::ChunkPut, 3, put)
            .write_to(&mut client, DEFAULT_MAX_PAYLOAD).unwrap();
        let stored = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(stored.kind, FrameKind::QueryResult);

        let mut get = Vec::new();
        get.extend_from_slice(&1u64.to_le_bytes());
        get.extend_from_slice(&(-20i32).to_le_bytes());
        get.extend_from_slice(&35i32.to_le_bytes());
        Frame::new(FrameKind::ChunkGet, 4, get.clone())
            .write_to(&mut client, DEFAULT_MAX_PAYLOAD).unwrap();
        let chunk = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(chunk.kind, FrameKind::ChunkData);
        assert_eq!(&chunk.payload[..16], &get[..]);
        assert_eq!(&chunk.payload[16..], &[0, 1, 0xff, 0x7f]);

        Frame::new(FrameKind::ChunkDelete, 5, get)
            .write_to(&mut client, DEFAULT_MAX_PAYLOAD).unwrap();
        let deleted = Frame::read_from(&mut reader, DEFAULT_MAX_PAYLOAD).unwrap();
        assert_eq!(deleted.kind, FrameKind::QueryResult);
        assert!(String::from_utf8(deleted.payload).unwrap().contains("\"affected_rows\":1"));

        drop(reader);
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
        assert!(validate_bind_address("127.0.0.1:7432", false).is_ok());
        assert!(validate_bind_address("[::1]:7432", false).is_ok());
        assert_eq!(
            validate_bind_address("0.0.0.0:7432", false).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(
            validate_bind_address("db.example.com:7432", false).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(validate_bind_address("0.0.0.0:7432", true).is_ok());
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
