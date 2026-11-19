# KSL and Minecraft integration

PostemberDB is being shaped for KSL workloads without making KSL a dependency of the database engine.

## KSL workload model

KSL already exposes asynchronous SQL operations, parameterized statements, bounded batches, and transaction-scoped sessions. Its documented default batch ceiling is 1,000 rows (configurable up to 5,000). PostemberDB's bulk API therefore accepts at most 5,000 rows per call and validates the entire batch before changing a table.

The current Rust API is:

```rust
use postemberdb::{Database, Value};

let mut db = Database::open("./data/game.pdb")?;
db.execute(
    "CREATE TABLE player_stats (uuid TEXT NOT NULL, coins INTEGER)"
)?;

let inserted = db.insert_many(
    "player_stats",
    vec![
        vec![Value::Text("player-a".into()), Value::Integer(100)],
        vec![Value::Text("player-b".into()), Value::Integer(250)],
    ],
)?;
assert_eq!(inserted, 2);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The operation checks row widths, column types, and NOT NULL constraints for every row before mutation. For a persistent database, it writes one snapshot for the entire batch rather than rewriting the file once per row. If saving fails, the in-memory rows added by that batch are rolled back.

## Minecraft-oriented data patterns

Good initial use cases include player statistics, economy ledgers, quest progress, playtime aggregates, and plugin-owned metadata.

- Use stable player UUIDs as identifiers; do not use player names as durable keys.
- Buffer frequent changes and flush bounded batches from a worker thread.
- Never perform disk I/O or database work on Paper's main tick thread.
- Keep query results bounded and map them to detached Kotlin values before returning to the game thread.
- For multiple Minecraft servers sharing the same live data, use a database-server deployment; do not point multiple processes at the same embedded database file.
- Keep periodic backups and test restore procedures.

## KSL addon integration (initial implementation)

The integration is implemented as an optional addon in this repository. It uses KSL's public addon/service API and exposes asynchronous query and write calls; it does not modify KSL core or its existing JDBC subsystem.

An initial separate addon now lives in integrations/kotlin-script-loader. It registers a postemberdb service through KSL's public addon API and exposes query/queryAsync/execute/executeAsync methods. It connects to the Rust server through the versioned JSONL/1 protocol, uses one short-lived connection per statement, and refuses non-loopback destinations. The upstream KotlinScriptLoader repository has not been modified.

This is not a JDBC driver and does not make PostemberDB selectable in KSL's existing database.type setting. Prepared statements, transaction-scoped client sessions, server-side row ceilings, cancellation, and end-to-end Paper/KSL tests remain pending. The current Rust TCP server remains loopback-only and is not suitable for untrusted networks.

## EF7 typed player profiles through the KSL addon

The optional addon now exposes EF7 profile operations on its existing loopback JSONL/1 connection. The Kotlin API includes `PostemberPlayerProfile`, `PostemberStoredPlayerProfile`, `PostemberPlayerProfileUpsert`, and `PostemberPlayerRemoval`, plus asynchronous `putPlayerProfileAsync`, `getPlayerProfileAsync`, `getPlayerProfileByNameAsync`, `deletePlayerAsync`, and `applyPlayerProfileBatchAsync` calls. Profile batches are committed through one EF7 CoW root publication and are preferred for bulk player saves. These operations use EF7's dedicated page-backed store and UUID/name indexes rather than SQL tables. See [the addon API guide](../integrations/kotlin-script-loader/README.md).

The bridge caps profile custom bytes and chunk payloads at 24 KiB because JSONL/1 hex-encodes binary data inside a 64 KiB line. The Kotlin service now exposes namespace create/get/find/list/delete and chunk put/get/delete. Namespace listing is capped at 64 items per request. Chunk reads check the persistent descriptor's recorded length before loading payload pages and reject payloads above the transport budget. The native Rust EF7 API continues to support records/chunks up to 64 MiB; larger chunk transport, chunk-range streaming, and atomic chunk batches remain pending until a bounded binary/framed protocol is added.

The JSONL extension commands include `EF7 NS_CREATE/NS_GET/NS_FIND/NS_LIST/NS_DELETE`, `EF7 CHUNK_PUT/CHUNK_GET/CHUNK_DELETE`, and `EF7 PROFILE_PUT/PROFILE_GET/PROFILE_GET_NAME/PLAYER_DELETE/PROFILE_BATCH`. `PROFILE_BATCH` carries a namespace ID, upsert/removal counts, typed profile tuples, and removal tuples; the server validates the complete bounded request before calling the single-commit EF7 batch API. Responses are one JSON line. The client rejects embedded newlines, checks the 64 KiB request size, and uses hex encoding for binary custom data.

EF7 commands are rejected while a SQL transaction session is open. SQL WAL and EF7 sidecar commits are separate durability domains; the server does not claim a cross-domain atomic transaction. The bridge remains loopback-only, without TLS/authentication, and is not a JDBC driver.

## Scaling roadmap

1. Atomic bounded batch writes (implemented in the Rust API).
2. WAL and transaction recovery, so writes do not require rewriting the complete database file.
3. Page-oriented storage and bounded-memory scans.
4. Secondary indexes for common predicates such as player UUID.
5. Query limits, prepared parameters, transaction-aware protocol support, and JDBC compatibility.
6. Load tests using realistic player counts, write rates, and restart/recovery scenarios.

Performance claims will be made only after reproducible benchmarks against the existing KSL SQLite path and the target deployment hardware.


## WAL and transaction state

The Rust engine now has an append-only write-ahead log. Each committed transaction is encoded as one checksummed frame and synced before the staged table state is published in memory. Recovery replays complete frames in sequence order and discards an incomplete final frame. A snapshot records the last applied WAL sequence, so checkpointing can safely replace the snapshot and truncate older log records without replaying them twice after a crash.

Rust transaction example:

```rust
use postemberdb::{Database, Value};

let mut db = Database::open("./data/game.pdb")?;
db.transaction(|tx| {
    tx.execute("INSERT INTO player_stats VALUES ('player-a', 100)")?;
    tx.execute("INSERT INTO player_stats VALUES ('player-b', 250)")?;
    Ok(())
})?;
db.checkpoint()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The current WAL records SQL statements, not physical page changes. It is an initial durability layer, not yet a high-throughput storage engine. The database still loads table rows into memory at startup, and there is not yet a cross-process file lock or concurrent writer protocol. Do not open one embedded database file from multiple server processes.


## Secondary indexes

The SQL subset now supports a single-column secondary index and equality lookup:

```sql
CREATE INDEX idx_player_uuid ON player_stats (uuid);
SELECT * FROM player_stats WHERE uuid = 'player-uuid';
```

Index definitions are stored in the snapshot and rebuilt from table rows on open. New inserts update the index. The first implementation is our engine-owned in-memory B+ tree from typed key values to row positions; it accelerates equality lookups but is not yet a page-backed persistent index, composite index, or range-scan planner.

## Minecraft workload benchmark

Run:

```bash
cargo bench --bench minecraft_workload
```

The benchmark inserts a generated player-statistics dataset in bounded batches, builds a UUID index, and times a set of equality lookups. Results depend on hardware and must be recorded alongside the machine, Rust version, dataset size, and commit SHA. Do not compare numbers from different machines as if they were controlled results.


## Fixed-size page snapshots

Snapshots are now stored as a PPG1 page file. The file uses fixed 4 KiB pages, page sequence identifiers, per-page checksums, and a whole-payload checksum. The decoder remains backward-compatible with the earlier raw PDB1, PDB2, PDB3, and PDB4 snapshot payloads.

This is the first physical page-format milestone, not yet the large-database memory solution: startup still reads the complete page file, reconstructs the serialized snapshot, and loads every table row into memory. The next storage task is a bounded page cache with lazy page reads and streaming scans; only after that can PostemberDB claim bounded-memory table access.


## Parameterized SQL API

The Rust API exposes typed positional parameters for the supported SQL subset:

```rust
let result = db.execute_params(
    "SELECT * FROM player_stats WHERE uuid = ?",
    &[Value::Text(player_uuid.to_owned())],
)?;
```

Parameters are encoded from typed `Value` variants and quoted by the engine; callers should not concatenate untrusted player input into SQL strings. The transaction facade also exposes `execute_params`. This is a Rust API capability, not yet a JDBC `PreparedStatement` implementation.

## Exact KSL adapter status

KSL's existing SQL backend still uses HikariCP and JDBC drivers for SQLite, MySQL, MariaDB, and PostgreSQL. PostemberDB is currently accessed through the separate PostemberDBKSL service addon, not through KSL's database.type setting. JSONL/1 is a versioned loopback protocol, not JDBC or a secure remote-network protocol. A JDBC driver, KSL dialect/migration support, and end-to-end compatibility tests remain future work.


## Client sessions and server transactions

The engine now exposes an owned `DatabaseSession`. A session works against a private staged view, can execute typed parameterized statements, and commits only if the database revision has not changed since the session began. This optimistic conflict check prevents silently overwriting another client's committed writes. The development TCP server exposes `BEGIN`, `COMMIT`, and `ROLLBACK` using this API. Sessions currently clone the in-memory table state; they are not yet suitable for a large number of concurrent clients or large databases. A production protocol and JDBC driver remain separate milestones.


## KSL schema and SQL compatibility work

The Rust SQL subset now accepts the core DDL patterns used by KSL's schema migrations: `CREATE TABLE IF NOT EXISTS`, `VARCHAR(n)`, `TEXT`/`LONGTEXT`, integer types, quoted identifiers, and composite primary keys. Composite primary-key uniqueness is enforced and its B-tree index can serve matching equality predicates. The insert path supports both `ON CONFLICT (...) DO UPDATE SET ... EXCLUDED.column` and `ON DUPLICATE KEY UPDATE ... VALUES(column)` upserts. Column-list inserts, projections, `UPDATE`, `DELETE`, and `AND`-joined equality predicates are also implemented.

This brings the engine closer to KSL's SQL usage, but does not complete the integration. The current engine still lacks a JDBC driver, full JDBC semantics, some SQL features used by arbitrary scripts, protocol-level row limits/cancellation, and a secure remote protocol. KSL itself has not been modified; the integration is a separate addon.


The Rust SQL layer also supports `COUNT(*) AS alias` for common aggregate lookups. It still does not implement the full SQL grammar or every feature used by arbitrary KSL scripts.


The SQL subset now accepts LIMIT and applies it while collecting matching row IDs, rather than materializing every result row first. This is useful for KSL's query row ceiling, although the engine still lacks streaming ResultSet pages and a server-side query timeout/cancellation contract.


## JSONL/1 protocol

The server keeps the legacy greeting and line protocol. A client opts into JSONL/1 by sending POSTEMBERDB JSONL/1 as its first command after the greeting. The server acknowledges with a single JSON line. Each following SQL statement produces exactly one JSON response line with ok, message/error, affected_rows, columns, and typed rows. Text values are JSON-escaped; integers, booleans, and NULL retain their JSON types.

The protocol still accepts one SQL statement per line, has no parameter array, and has no transaction session spanning separate client connections. The current addon uses one connection per operation. Keep the database bound to loopback; TLS and authentication are not implemented.

Build and installation instructions for the addon are in integrations/kotlin-script-loader/README.md.
