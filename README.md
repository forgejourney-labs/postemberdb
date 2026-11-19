# PostemberDB

**PostemberDB** is an independent SQL-like database engine written in Rust. It is designed around two ways to use the same engine:

- **Embedded** — link the Rust library directly into an application.
- **Database Server** — run the engine as a separate process and connect over a network protocol.

PostemberDB is not a wrapper around SQLite or PostgreSQL. Its storage, execution, and protocol layers are intended to be implemented in this project.

## KSL and Minecraft integration

KSL remains optional. The Rust core now includes an atomic, bounded bulk-insert API (up to 5,000 rows per call) so persistent storage is written once per batch. This fits common KSL and Minecraft workloads such as player statistics and economy updates. A separate optional KSL addon is included in [integrations/kotlin-script-loader](integrations/kotlin-script-loader/README.md). It exposes an asynchronous service over the versioned loopback JSONL/1 protocol; it is not a JDBC driver or a selectable KSL SQL dialect yet. See [KSL and Minecraft integration](docs/KSL_MINECRAFT.md) for its limits.

## Storage architecture

Our native B+ tree and evolving tree-oriented storage engine are named **EmberTree**. The in-memory B+ tree backs SQL equality indexes using stable table-local row IDs; ordinary INSERT, UPDATE, DELETE, and UPSERT operations touch only affected index keys. A persistent CoW tree mirrors supported SQL index entries in EPG1 pages with EAM3 root metadata and a dedicated redo WAL. Supported equality lookups use this mirror when valid and fall back to in-memory indexes otherwise. SQL commits feed the mirror through a row-level mutation journal rather than rescanning every index. The file-backed extent layer now has a bounded per-handle LRU cache for validated EPG1 page payloads; KeyValueStore and the new variable-length RecordStore can configure capacity and inspect hit/miss/eviction counters. RecordStore uses multi-page extents for records up to 64 MiB and an EmberTree B+ Tree descriptor index. It is an independent sidecar, not yet the canonical SQL row store. SQL rows and catalog metadata are still stored in the main snapshot/WAL and reconstructed in memory. Design and recovery boundaries are documented in [docs/EMBER_TREE.md](docs/EMBER_TREE.md).

## EmberForge 7 — Minecraft/KSL storage

**EmberForge 7 (EF7)** is a dedicated persistent record engine for Minecraft servers and KotlinScriptLoader. It uses independent `.ef7.pages`, `.ef7.wal`, and `.ef7.lock` sidecars over the PDB6 extent-page allocator and CoW B+ tree. EF7 currently provides hierarchical server/world/plugin namespaces, typed Minecraft player profiles with UUID and case-insensitive username indexes, bounded player scans, signed-coordinate chunk storage, rectangular chunk reads, and atomic player/chunk batches. Large payloads use checksummed overflow pages up to 64 MiB per record.

EF7 is an independent storage engine at this stage. The optional KSL addon exposes typed player-profile CRUD over the loopback JSONL/1 bridge (24 KiB custom-data transport limit); it is not a JDBC driver or KSL SQL dialect. EF7 does **not** replace the canonical PDB5 SQL snapshot/WAL and does not yet provide cross-domain SQL/KV/Registry transactions. See [EmberForge 7 documentation](docs/EMBERFORGE7.md), [KSL bridge API](integrations/kotlin-script-loader/README.md), and [implementation gates](docs/IMPLEMENTATION_STATUS.md).

## Current status

The project is at an early development stage. The first prototype provides:

- an in-memory database for `Database::new()` and a persistent database opened with `Database::open(path)`; `.pbd` is the recommended extension, while existing `.db` paths remain accepted for legacy files;
- a growing SQL subset: `CREATE TABLE IF NOT EXISTS`, column-list `INSERT`, projections, `WHERE` equality predicates joined by `AND`, bounded `LIMIT`, `UPDATE`, `DELETE`, `CREATE INDEX`, and PostgreSQL/MariaDB-style upserts;
- integer, text, boolean, and nullable values;
- basic column type, `NOT NULL`, and composite primary-key validation;
- a versioned PPG1 fixed-4-KiB-page snapshot format with per-page and whole-payload checksums;
- an EmberTree page-backed CoW B+ tree using EPG1 pages, EAM3 root metadata, and a dedicated redo WAL; supported SQL equality indexes are mirrored to it and queried from it when the mirror is valid;
- an append-only, checksummed WAL with transaction frames, recovery replay, incomplete-tail truncation, and sequence-aware checkpoints;
- staged transaction sessions with optimistic conflict detection and SQL `BEGIN`/`COMMIT`/`ROLLBACK` in the development server;
- secondary indexes backed by the native in-memory B+ tree and stable table-local row IDs;
- incremental index-key maintenance for INSERT, UPDATE, DELETE, and UPSERT; DELETE compacts table rows without rebuilding index trees;
- direct row-level SQL index mutation journaling for INSERT, UPDATE, DELETE, UPSERT, and CREATE INDEX, applied in one mixed CoW batch after the canonical SQL WAL commit;
- a standalone persistent byte-oriented `KeyValueStore` with prefix scans, bounded batches, atomic prefix deletion, bounded per-call cleanup, and a configurable page cache with runtime statistics;
- an independent page-backed `RecordStore` for variable-length records up to 64 MiB, indexed by stable caller-supplied IDs; bounded multi-record batches use one descriptor-tree publication, but it is not yet wired as SQL's canonical row store;
- a typed `SqlRowStore` prototype for SQL row payloads, using SRW1 records over multi-page EPG1 storage with stable-ID lookup, bounded scans, and atomic mixed upsert/delete batches; see [docs/SQL_ROW_STORE.md](docs/SQL_ROW_STORE.md);
- a typed hierarchical `Registry` with hives, nested keys, named values, case-insensitive paths, and String/Integer/Boolean/Binary/JSON value types;
- temporary-file writes followed by rename when checkpointing the database;
- an OS-level exclusive sidecar lock preventing a second process/handle from opening the same database path;
- atomic bulk insertion of up to 5,000 typed rows with one persistence operation per batch;
- a loopback-only TCP server with a versioned JSONL/1 protocol for the optional KSL addon.

**Current limitations:** SQL table rows and catalog metadata are still stored in memory after snapshot/WAL replay; the page cache, lazy row-page access, and page-backed SQL table storage are not implemented. The persistent SQL index mirror is derived and rebuilt on open; unsupported index-key encodings use the in-memory fallback. KV and Registry use separate page/WAL sidecars and are not part of the SQL transaction's cross-domain atomic commit. Large values need an overflow-page/blob layer. The server protocol remains loopback-only and has no TLS, authentication, or authorization. JSONL/1 is an initial loopback-only integration protocol, not a secure remote protocol. Composite primary-key uniqueness is supported. The SQL parser is deliberately small; this is not yet a production database, and the server protocol is not yet suitable for untrusted networks; it has no TLS, authentication, or authorization.

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

Detailed implementation status and acceptance criteria: [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md). See also [Native Key-Value and Registry](docs/KEY_VALUE_AND_REGISTRY.md), [RecordStore](docs/RECORD_STORE.md), and [EmberTree](docs/EMBER_TREE.md).

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
