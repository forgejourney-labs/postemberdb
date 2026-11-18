# Implementation status

This file tracks implemented behavior separately from planned work. A checked box means code and focused tests have been added; it does not mean the feature is production-ready.

## 1. WAL and transactions

- [x] Append-only WAL frames with checksums.
- [x] One WAL frame per committed transaction.
- [x] Sync the WAL before publishing staged writes in memory.
- [x] Replay complete frames in sequence order during open.
- [x] Discard and truncate an incomplete final frame.
- [x] Sequence-aware snapshot checkpointing to avoid duplicate replay after a crash.
- [x] In-memory transaction rollback when the callback returns an error.
- [x] Owned client sessions with optimistic revision conflict detection.
- [x] Development server `BEGIN`, `COMMIT`, and `ROLLBACK` commands.
- [ ] WAL streaming recovery (the current implementation reads the WAL into memory).
- [ ] Savepoints, explicit SQL BEGIN/COMMIT/ROLLBACK, and transaction isolation levels.
- [ ] Multi-process file locking and concurrent-writer coordination.
- [ ] Fault-injection tests for power loss at every write/sync/rename boundary.

## 2. Indexes

- [x] Single-column typed B-tree equality indexes.
- [x] CREATE INDEX and SELECT equality predicates.
- [x] Index maintenance during INSERT and bounded bulk insert.
- [x] Persist index definitions and rebuild index entries after snapshot load.
- [x] Restore index definitions through WAL replay.
- [x] Unique composite primary-key indexes for SQL PRIMARY KEY constraints.
- [ ] General composite indexes, range scans, ordering, and index selection for multi-predicate queries.
- [ ] Range scans, ordering, and index selection for multi-predicate queries.
- [ ] Cost-aware query planner and EXPLAIN output.

## 3. Page-oriented storage

- [x] Fixed 4 KiB page-file snapshots.
- [x] Page identifiers, per-page checksums, and whole-payload checksum.
- [x] Backward-compatible reads for the earlier raw PDB1/PDB2/PDB3 snapshot payloads.
- [ ] Bounded page cache / buffer pool.
- [ ] Lazy page reads and streaming table scans.
- [ ] Page-level row updates instead of full snapshot rewrite at checkpoint.
- [ ] Free-space management, compaction, and crash-safe page allocation.

The current page format is a physical layout milestone only. Startup still reconstructs the complete snapshot and loads all rows into memory.

## 4. KSL/JVM adapter

- [x] Typed positional SQL parameters in the Rust API.
- [x] Typed positional parameters inside transaction sessions.
- [x] Bounded bulk insert API aligned with KSL's configurable batch ceiling.
- [x] Transaction callback API for Rust callers.
- [ ] Stable framed client/server protocol.
- [ ] JDBC driver and service-provider registration.
- [ ] KSL dialect, configuration, migration, and connection-pool integration.
- [ ] PreparedStatement, ResultSet, timeout, row-limit, and cancellation compatibility tests.
- [ ] End-to-end tests using the actual KSL plugin.

KSL currently uses HikariCP and JDBC; PostemberDB is not yet a selectable KSL backend.

## 5. Minecraft workloads and performance

- [x] Reproducible 100,000-row player-statistics benchmark harness.
- [x] Bulk insert and UUID equality lookup workload.
- [x] CI compiles the benchmark target.
- [ ] Execute and record baseline benchmark results on known hardware.
- [ ] Compare against KSL's current SQLite path on the same machine and dataset.
- [ ] Restart/recovery stress tests with large WALs.
- [ ] Concurrent player-update and read-heavy workload tests.
- [ ] Memory ceiling, tail latency, and queue-backpressure measurements.

## Release gate

Do not label PostemberDB production-ready until the page cache, transaction/recovery tests, network security, JDBC/KSL integration, backup/restore process, and repeatable performance tests are complete.
