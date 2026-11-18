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
