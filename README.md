# PostemberDB

**Release identity:** PostemberDB 1 — **Ember**. `1 Ember` is the codename series, not a claim that the current engine is SemVer `1.0.0`; the Cargo package version remains the compatibility/build version.

**PostemberDB** is an independent SQL-like database engine written in Rust. It is designed around two ways to use the same engine:

- **Embedded** — link the Rust library directly into an application.
- **Database Server** — run the engine as a separate process and connect over a network protocol.

PostemberDB is not a wrapper around SQLite or PostgreSQL. Its storage, execution, and protocol layers are intended to be implemented in this project.

## KSL and Minecraft integration

KSL remains optional. The Rust core includes bounded bulk operations and typed positional SQL parameters. The separate addon in [integrations/kotlin-script-loader](integrations/kotlin-script-loader/README.md) exposes asynchronous SQL calls, typed `?` parameters, atomic parameterized transaction batches (up to 500 statements), and a bounded pool of reusable loopback JSONL/1 connections. KSL scripts should use `queryParamsAsync` / `executeParamsAsync` for dynamic values and `transactionAsync { ... }` when multiple SQL statements must commit together; no SQL text interpolation is needed. The addon itself is not a JDBC driver or a selectable KSL `database.type` dialect. A separate experimental JDBC 4.2 baseline lives in [jdbc-driver](jdbc-driver/) and still lacks server-side prepared statements, streamed results, and full JDBC compliance. See [KSL adapter status](docs/KSL_ADAPTER_STATUS.md), [KSL and Minecraft integration](docs/KSL_MINECRAFT.md), and [JDBC driver notes](docs/JDBC_DRIVER.md).

## Storage architecture

The native in-memory B+ tree backs SQL equality indexes with stable table-local row IDs, while **EmberTree** provides the persistent copy-on-write page tree, EPG1 data pages, EAM3 allocator/root metadata, and its redo WAL. After explicit PDB6 creation or migration, the SQL catalog, row descriptors, row payload pages, index mutations, and manifest share one PDB6 page-tree root. PDB6 startup loads bounded catalog metadata rather than hydrating every row; SELECT and table-page reads stream rows from disk in bounded batches. Simple single-statement DML also has a bounded page-native path. Multi-statement transaction compatibility paths still hydrate participating tables, and a shared SQL buffer pool with page pinning remains unfinished. The file-backed extent layer includes a bounded per-handle LRU page cache. `KeyValueStore` and the variable-length `RecordStore` are separate sidecar stores; `RecordStore` supports records up to 64 MiB but is not the SQL executor's canonical row store. See [PDB6 storage](docs/PDB6_CANONICAL_SQL_STORAGE.md), [EmberTree](docs/EMBER_TREE.md), and [implementation status](docs/IMPLEMENTATION_STATUS.md).

## EmberForge 7 — Minecraft/KSL storage

**EmberForge 7 (EF7)** is a dedicated persistent record engine for Minecraft servers and KotlinScriptLoader. It uses independent `.ef7.pages`, `.ef7.wal`, and `.ef7.lock` sidecars over the PDB6 extent-page allocator and CoW B+ tree. EF7 currently provides hierarchical server/world/plugin namespaces, typed Minecraft player profiles with UUID and case-insensitive username indexes, bounded player scans, signed-coordinate chunk storage, rectangular chunk reads, and atomic player/chunk batches. Large payloads use checksummed overflow pages up to 64 MiB per record.

EF7 is a separate durable data domain using `.ef7.pages`, `.ef7.wal`, and `.ef7.lock` sidecars over the PDB6 extent allocator and CoW tree. It provides hierarchical namespaces, typed Minecraft player profiles with UUID and case-insensitive username indexes, chunk CRUD and bounded rectangular reads, player/chunk batches, generic records up to 64 MiB in the native Rust API, and ordered TTL expiry with explicit bounded sweeps. The KSL JSONL/1 bridge is loopback-only and caps hex-encoded profile/chunk/record payloads at 24 KiB; the opt-in Binary/1 transport currently carries single-frame chunk payloads up to 8 MiB. Durable prepared-participant adapters exist for SQL, KV, Registry, and EF7, but ordinary SQL transactions and KSL `transactionAsync` do not automatically enlist all domains; general cross-domain transaction control is not exposed through Binary/1 or KSL yet. See [EmberForge 7](docs/EMBERFORGE7.md), [EF7 TTL](docs/EF7_TTL.md), and [implementation status](docs/IMPLEMENTATION_STATUS.md).

## Current status

PostemberDB is an experimental Rust database engine. It is not production-ready. The implementation currently includes:

- An in-memory database through `Database::new()`, legacy PDB5 persistence, and page-backed PDB6 persistence. `.pbd` is the recommended file extension; existing `.db` files remain readable for compatibility.
- A growing SQL subset: table creation, inserts, projections, typed values, primary and secondary indexes, equality and bounded range scans, `WHERE` comparisons, `LIKE`, `IN`, `BETWEEN`, NULL-aware predicates, nested boolean logic, checked integer arithmetic, selected scalar functions, bounded `LIMIT`, `ORDER BY`, updates, deletes, and PostgreSQL/MariaDB-style upserts. This is not full PostgreSQL or SQLite SQL compatibility.
- PDB6 catalog records, row descriptors, overflow row pages, index entries, and manifest published through a shared CoW B+ Tree root. PDB6 is authoritative after explicit migration; new PDB6 databases can be created directly. Unmigrated legacy databases remain PDB5-backed.
- Metadata-only PDB6 open, bounded streaming SELECT/table-page reads, early LIMIT termination, streaming `COUNT(*)`, and page-native bounded single-statement INSERT/UPDATE/DELETE. Multi-statement transactions, `DatabaseSession`, and UPSERT still use compatibility hydration paths.
- A checksummed append-only SQL WAL, a separate EmberTree redo WAL, durable EAM3 allocator metadata, B+ Tree recovery validation, named PDB6 snapshots, and conservative vacuum/reclamation.
- A native in-memory B+ Tree for SQL indexes, persistent CoW tree pages, a standalone byte-oriented `KeyValueStore`, a typed `Registry`, a variable-length `RecordStore`, and the specialized EmberForge 7 Minecraft record engine.
- An embedded Rust API, a loopback-only development server, an authenticated TLS server with Argon2id authentication and RBAC, a versioned Binary/1 operator protocol, and a JSONL/1 loopback compatibility protocol.
- A separate KSL addon with asynchronous typed SQL parameters and atomic SQL statement batches, plus an experimental JDBC 4.2 baseline. The JDBC driver is not yet fully JDBC-compliant and still uses client-side parameter binding.
- Separate Browser and Admin operator applications. Their CI checks currently compile and test those applications, but this does not replace live production-like integration testing.

**Important limitations:** PDB6 still lacks a shared bounded SQL buffer pool and page pinning. Multi-statement SQL transactions and selected compatibility operations can hydrate whole participating tables; UPDATE/DELETE predicate selection still scans rows. The cost-aware query planner, full SQL expression grammar, general composite secondary indexes, server-side prepared-statement handles, streaming protocol results, multi-frame large-record transport, request cancellation, mutual TLS, per-user resource quotas, and backup/restore workflow are unfinished. PDB6 vacuum conservatively retains overflow pages while named snapshots exist; active-reader epochs, fine-grained per-snapshot overflow tracing, and crash-resumable vacuum remain open. KV, Registry, SQL, and EF7 have durable coordinator participant adapters, but normal SQL/KSL transactions do not automatically span all four domains. See [implementation gates](docs/IMPLEMENTATION_STATUS.md) and [security gates](docs/SECURITY_MODEL.md).

## CLI

Create a new page-backed SQL database with `cargo run -- create-pdb6 ./data/postemberdb.pbd`. Existing PDB5 databases can be explicitly promoted with `cargo run -- migrate-pdb6 ./data/postemberdb.pbd`; the original PDB5 snapshot is retained.

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

The plain `server` command opens the specified database file (default: `postemberdb.pbd`) and binds only to loopback. It is intended for local development and accepts SQL line commands including `BEGIN`, `COMMIT`, and `ROLLBACK`.

For remote authenticated connections, start the TLS server with a certificate and private key:

~~~bash
cargo run -- server-tls 0.0.0.0:7432 ./data/postemberdb.pbd ./cert.pem ./key.pem
~~~

Provision a user with `postemberdb user add`, then connect with a client that trusts the certificate authority and verifies the certificate's DNS name. The Rust Browser and Admin operator tools use Binary/1 through the verified TLS client. The KSL addon retains its existing JSONL/1 compatibility path; it is not the secure remote operator client or a JDBC driver.

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

The near-term order is reliability first, then performance and compatibility:

1. Expand crash-point and subprocess fault-injection coverage across PDB6, the SQL WAL, EmberTree redo, allocator metadata, the cross-domain coordinator, and vacuum/reclamation.
2. Finish the shared bounded SQL buffer pool, page pinning, memory budgets, and bounded multi-statement transaction staging.
3. Improve the query planner, persistent index range planning, predicate pushdown, composite secondary indexes, and SQL expression/statement coverage.
4. Add reader epochs/pins, per-snapshot overflow reachability, crash-safe vacuum progress, and verified backup/restore.
5. Complete the Binary/1 transaction and streaming protocol, server-side prepared statements, cancellation, backpressure, and large EF7 payload streaming.
6. Bring JDBC closer to full compliance and validate the KSL addon against a real Paper/KSL runtime.
7. Record repeatable Minecraft workload results and compare the same dataset and hardware against the existing SQLite path.

The exact acceptance criteria and current implementation checkboxes live in [docs/IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md). PDB6 format and migration behavior are specified in [docs/PDB6_CANONICAL_SQL_STORAGE.md](docs/PDB6_CANONICAL_SQL_STORAGE.md).

## Development branches

- `dev` — newest active development changes and fixes.
- `canary` — early validation of implemented features.
- `unstable` — polished release candidates that are not yet release-ready.
- `stable` — supported releases that passed the full release and security gates.

Promotion policy, required checks, and the rule against automatic merges between diverged histories are documented in [docs/BRANCH_POLICY.md](docs/BRANCH_POLICY.md).


See [JDBC Driver](docs/JDBC_DRIVER.md) for Maven setup, TLS configuration, connection properties and current JDBC feature limits.

## Operator applications

Two separate Rust tools are being developed under [`tools/`](tools/README.md):

- **[PostemberDB Browser](tools/browser/README.md)** — a native desktop SQL browser with a compact classic 2014–2015-style interface.
- **[PostemberDB Admin](tools/admin/README.md)** — a local phpMyAdmin-inspired web administration panel.

Both tools support local embedded access, live catalog browsing, paging, SQL, and remote TLS connections through the authenticated Binary/1 client. Browser also supports row forms and transactional updates; Admin provides remote server status, user/role management, password reset, and audit browsing. The web Admin listener itself remains loopback-only and must not be published through a reverse proxy.
