# PostemberDB

**PostemberDB** is an independent SQL-like database engine written in Rust. It is designed around two ways to use the same engine:

- **Embedded** — link the Rust library directly into an application.
- **Database Server** — run the engine as a separate process and connect over a network protocol.

PostemberDB is not a wrapper around SQLite or PostgreSQL. Its storage, execution, and protocol layers are intended to be implemented in this project.

## KSL and Minecraft integration

KSL remains optional. The Rust core now includes an atomic, bounded bulk-insert API (up to 5,000 rows per call) so persistent storage is written once per batch. This fits common KSL and Minecraft workloads such as player statistics and economy updates. A separate optional KSL addon is included in [integrations/kotlin-script-loader](integrations/kotlin-script-loader/README.md). It exposes an asynchronous service over the versioned loopback JSONL/1 protocol; it is not a JDBC driver or a selectable KSL SQL dialect yet. See [KSL and Minecraft integration](docs/KSL_MINECRAFT.md) for its limits.

## Storage architecture

The planned tree-oriented storage model is named **EmberTree**. Its design, CoW/in-place modes, manual defragmentation contract, page budgets, compression, snapshots, and recovery invariants are documented in [docs/EMBER_TREE.md](docs/EMBER_TREE.md). EmberTree is a design target; the current PPG1 snapshot implementation is not yet that engine. Security posture and release gates are in [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md).

## Current status

The project is at an early development stage. The first prototype provides:

- an in-memory database for `Database::new()` and a persistent database opened with `Database::open(path)`; `.pbd` is the recommended extension, while existing `.db` paths remain accepted for legacy files;
- a growing SQL subset: `CREATE TABLE IF NOT EXISTS`, column-list `INSERT`, projections, `WHERE` equality predicates joined by `AND`, bounded `LIMIT`, `UPDATE`, `DELETE`, `CREATE INDEX`, and PostgreSQL/MariaDB-style upserts;
- integer, text, boolean, and nullable values;
- basic column type, `NOT NULL`, and composite primary-key validation;
- a versioned PPG1 fixed-4-KiB-page snapshot format with per-page and whole-payload checksums;
- an append-only, checksummed WAL with transaction frames, recovery replay, incomplete-tail truncation, and sequence-aware checkpoints;
- staged transaction sessions with optimistic conflict detection and SQL `BEGIN`/`COMMIT`/`ROLLBACK` in the development server;
- secondary indexes backed by our in-memory B+ tree with indexed equality predicates (`CREATE INDEX` and `SELECT * ... WHERE column = value`);
- temporary-file writes followed by rename when checkpointing the database;
- an OS-level exclusive sidecar lock preventing a second process/handle from opening the same database path;
- atomic bulk insertion of up to 5,000 typed rows with one persistence operation per batch;
- a loopback-only TCP server with a versioned JSONL/1 protocol for the optional KSL addon.

**Current limitations:** WAL transactions and optimistic client sessions are implemented, but the engine still replays SQL records and stores table rows in memory; page-cache and streaming row access (the page snapshot is still decoded into memory), range indexes and general composite indexes, bounded query execution, concurrent multi-process file locking, and a secure production network protocol are not implemented. JSONL/1 is an initial loopback-only integration protocol. JSONL/1 is an initial loopback-only integration protocol, not a secure remote protocol. Composite primary-key uniqueness is supported. The SQL parser is deliberately small; this is not yet a production database, and the server protocol is not yet suitable for untrusted networks; it has no TLS, authentication, or authorization.

## CLI

See [docs/CLI.md](docs/CLI.md) for available commands and the planned operator command groups.

## Build and test

Requires a recent stable Rust toolchain.

```bash
cargo test
cargo build --release
cargo bench --no-run
```

## Run the server

```bash
cargo run -- server 127.0.0.1:7432
```

The server opens the specified database file (default: `postemberdb.pbd`) and persists successful writes automatically. The development protocol accepts one SQL statement per line and supports `BEGIN`, `COMMIT`, `ROLLBACK`, and `QUIT`. The server supports the versioned JSONL/1 protocol for the optional KSL addon; it is not JDBC and is not suitable for untrusted networks.

## Use as an embedded library

```rust
use postemberdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::open("./data/app.pbd")?;
    db.execute("CREATE TABLE users (id INTEGER NOT NULL, name TEXT)")?;
    db.execute("INSERT INTO users VALUES (1, 'Ada')")?;

    let result = db.execute("SELECT * FROM users")?;
    println!("{}", result.message);
    Ok(())
}
```

## Roadmap

Detailed implementation status and acceptance criteria: [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md).

1. Replace the prototype parser with a tokenizer and structured SQL AST.
2. Add a bounded page cache and lazy page reads over the fixed-size page file.
3. Expand WAL transactions with savepoints, checkpoint scheduling, and recovery stress tests.
4. Add streaming table scans and bounded query result materialization.
5. Extend indexes to range/composite keys and add a cost-aware query planner.
6. Replace the development line protocol with a framed, parameter-aware server protocol.
7. Add a stable KSL/JVM JDBC adapter and benchmark it against the general API.
8. Run the Minecraft workload benchmark with `cargo bench --bench minecraft_workload`.

## Development branches

- `dev` — newest active development changes.
- `canary` — raw but implemented features under early testing.
- `unstable` — implemented and polished, but not release-ready.
- `stable` — thoroughly validated, reliable, and security-reviewed releases.
