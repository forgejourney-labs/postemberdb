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

## Planned KSL adapter

The next integration layer should expose the Rust engine through a supported JVM boundary and adapt it to KSL's asynchronous database API. The adapter must preserve KSL's runtime-generation cancellation, query timeouts, result-row limits, batch limits, and transaction semantics. It must not expose raw pointers, Rust-owned buffers, or live database cursors to Kotlin scripts.

This adapter is not implemented yet. The current release is a Rust library and a development TCP server, not a JDBC driver and not a drop-in replacement for SQLite, MySQL, MariaDB, or PostgreSQL.

## Scaling roadmap

1. Atomic bounded batch writes (implemented in the Rust API).
2. WAL and transaction recovery, so writes do not require rewriting the complete database file.
3. Page-oriented storage and bounded-memory scans.
4. Secondary indexes for common predicates such as player UUID.
5. Query limits, prepared parameters, and a stable KSL/JVM adapter.
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

Index definitions are stored in the snapshot and rebuilt from table rows on open. New inserts update the index. The first implementation is an in-memory B-tree map from typed key values to row positions; it accelerates equality lookups but is not yet a page-backed persistent index, composite index, or range-scan planner.

## Minecraft workload benchmark

Run:

```bash
cargo bench --bench minecraft_workload
```

The benchmark inserts a generated player-statistics dataset in bounded batches, builds a UUID index, and times a set of equality lookups. Results depend on hardware and must be recorded alongside the machine, Rust version, dataset size, and commit SHA. Do not compare numbers from different machines as if they were controlled results.


## Fixed-size page snapshots

Snapshots are now stored as a PPG1 page file. The file uses fixed 4 KiB pages, page sequence identifiers, per-page checksums, and a whole-payload checksum. The decoder remains backward-compatible with the earlier raw PDB1, PDB2, and PDB3 snapshot payloads.

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

KSL currently initializes its SQL backend through HikariCP and JDBC drivers. Its published dialect set is SQLite, MySQL, MariaDB, and PostgreSQL; PostemberDB is not yet a selectable KSL dialect. A production adapter therefore still needs a stable server protocol, a JDBC driver, and KSL dialect/migration support. The current Rust TCP line server is only a development interface and must not be presented as the KSL adapter.
