# PostemberDB

**PostemberDB** is an independent SQL-like database engine written in Rust. It is designed around two ways to use the same engine:

- **Embedded** — link the Rust library directly into an application.
- **Database Server** — run the engine as a separate process and connect over a network protocol.

PostemberDB is not a wrapper around SQLite or PostgreSQL. Its storage, execution, and protocol layers are intended to be implemented in this project.

## KSL and Minecraft integration

KSL remains optional. The Rust core now includes an atomic, bounded bulk-insert API (up to 5,000 rows per call) so persistent storage is written once per batch. This fits common KSL and Minecraft workloads such as player statistics and economy updates. A JVM/JDBC-compatible adapter is not implemented yet; see [KSL and Minecraft integration](docs/KSL_MINECRAFT.md) for the current contract and roadmap.

## Current status

The project is at an early development stage. The first prototype provides:

- an in-memory database for `Database::new()` and a persistent database opened with `Database::open(path)`;
- basic `CREATE TABLE`, `INSERT INTO ... VALUES`, and `SELECT * FROM` statements;
- integer, text, boolean, and nullable values;
- basic column type and `NOT NULL` validation;
- a versioned, fixed-4-KiB-page snapshot format with per-page and whole-payload checksums;
- an append-only, checksummed WAL with transaction frames, recovery replay, incomplete-tail truncation, and sequence-aware checkpoints;
- secondary B-tree indexes with indexed equality predicates (`CREATE INDEX` and `SELECT * ... WHERE column = value`);
- temporary-file writes followed by rename when saving the database;
- atomic bulk insertion of up to 5,000 typed rows with one persistence operation per batch;
- a minimal line-oriented TCP server for development and experimentation.

**Current limitations:** WAL transactions are implemented, but the engine still replays SQL records and stores table rows in memory; page-cache and streaming row access (the page snapshot is still decoded into memory), range/composite indexes, bounded query execution, concurrent multi-process file locking, and a production network protocol are not implemented. The SQL parser is deliberately small; this is not yet a production database, and the server protocol is not yet suitable for untrusted networks.

## Build and test

Requires a recent stable Rust toolchain.

```bash
cargo test
cargo build --release
```

## Run the server

```bash
cargo run -- server 127.0.0.1:7432
```

The server opens the specified database file (default: `postemberdb.pdb`) and persists successful writes automatically. The development protocol accepts one SQL statement per line. Send `QUIT` to close a client session.

## Use as an embedded library

```rust
use postemberdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::open("./data/app.pdb")?;
    db.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Ada')")?;

    let result = db.execute("SELECT * FROM users")?;
    println!("{}", result.message);
    Ok(())
}
```

## Roadmap

1. Replace the prototype parser with a tokenizer and structured SQL AST.
2. Evolve the initial file snapshot format into page-oriented storage.
3. Expand WAL transactions with savepoints, checkpoint scheduling, and recovery stress tests.
4. Add a bounded page cache and streaming table scans over the fixed-size page store.
5. Extend indexes to range/composite keys and add a cost-aware query planner.
6. Replace the development line protocol with a framed, parameter-aware server protocol.
7. Add a stable KSL/JVM JDBC adapter and benchmark it against the general API.
8. Run the Minecraft workload benchmark with `cargo bench --bench minecraft_workload`.

## Development branches

- `dev` — active development.
- `canary` — early integration builds.
- `unstable` — experimental work.
- `stable` — reviewed releases.
