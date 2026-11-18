# Implementation status

This file tracks implemented behavior separately from planned work. A checked box means code and focused tests have been added; it does not mean the feature is production-ready.

## 1. WAL and transactions

- [x] Append-only WAL frames with checksums.
- [x] One WAL frame per committed transaction.
- [x] Sync the WAL before publishing staged writes in memory.
- [x] Replay complete frames in sequence order during open.
- [x] Apply each WAL transaction atomically during recovery; failed frames cannot publish partial table state.
- [x] Discard and truncate an incomplete final frame.
- [x] Sequence-aware snapshot checkpointing to avoid duplicate replay after a crash.
- [x] In-memory transaction rollback when the callback returns an error.
- [x] Owned client sessions with optimistic revision conflict detection.
- [x] Development server `BEGIN`, `COMMIT`, and `ROLLBACK` commands.
- [x] Streaming WAL recovery with a bounded 64 MiB maximum transaction frame; the complete WAL is not loaded into memory.
- [x] Stage the recovery pass once and publish only after all complete frames validate, avoiding a full database clone per frame.
- [x] Focused WAL tests for cross-frame atomicity, checksum corruption, sequence gaps, and truncated payloads.
- [ ] Savepoints, explicit SQL BEGIN/COMMIT/ROLLBACK, and transaction isolation levels.
- [x] OS-level exclusive sidecar lock for the same database path.
- [ ] Hard-link alias detection and broader multi-writer coordination.
- [ ] Fault-injection tests for power loss at every write/sync/rename boundary.

## 2. Indexes

- [x] Single-column typed B-tree equality indexes.
- [x] CREATE INDEX and SELECT equality predicates.
- [x] Index maintenance during INSERT and bounded bulk insert.
- [x] Persist index definitions and rebuild index entries after snapshot load.
- [x] Restore index definitions through WAL replay.
- [x] Unique composite primary-key indexes for SQL PRIMARY KEY constraints.
- [ ] User-defined general composite indexes, range scans, ordering, and a cost-aware index planner.
- [ ] Cost-aware query planner and EXPLAIN output.

## 3. Page-oriented storage

- [x] Fixed 4 KiB page-file snapshots.
- [x] Page identifiers, per-page checksums, and whole-payload checksum.
- [x] Backward-compatible reads for earlier raw PDB1/PDB2/PDB3 snapshots plus current PDB4 payloads inside PPG1 page files.
- [ ] Bounded page cache / buffer pool.
- [ ] Lazy page reads and streaming table scans.
- [ ] Page-level row updates instead of full snapshot rewrite at checkpoint.
- [ ] Free-space management, compaction, and crash-safe page allocation.

The current page format is a physical layout milestone only. Startup still reconstructs the complete snapshot and loads all rows into memory.

## 4. KSL/JVM adapter

- [x] Typed positional SQL parameters in the Rust API.
- [x] Typed positional parameters inside transaction sessions.
- [x] SQL LIMIT clause applied during index lookup and table scans.
- [ ] TTL expiry index and explicitly budgeted expiration sweeps.
- [x] Bounded bulk insert API aligned with KSL's configurable batch ceiling.
- [x] Transaction callback API for Rust callers.
- [ ] Stable framed client/server protocol.
- [ ] JDBC driver and service-provider registration.
- [ ] KSL dialect, configuration, migration, and connection-pool integration.
- [ ] PreparedStatement, ResultSet, timeout, row-limit, and cancellation compatibility tests.
- [ ] End-to-end tests using the actual KSL plugin.

KSL currently uses HikariCP and JDBC; PostemberDB is not yet a selectable KSL backend.

## 5. Security and operations

- [x] Development server rejects non-loopback bind addresses until TLS/authentication exist.
- [x] Bounded client count, SQL line size, UTF-8 input, and socket timeouts.
- [x] CLI recovery check and WAL checkpoint commands.
- [ ] TLS, authentication, role-based authorization, and audit log.
- [ ] Encryption at rest and managed key rotation.
- [ ] Verified full/incremental backup and restore.
- [ ] Fuzzing, corruption tests, and fault-injection release suite.

## 6. Minecraft workloads and performance

- [x] Reproducible 100,000-row player-statistics benchmark harness.
- [x] Bulk insert and UUID equality lookup workload.
- [x] CI compiles the benchmark target.
- [ ] Execute and record baseline benchmark results on known hardware.
- [ ] Compare against KSL's current SQLite path on the same machine and dataset.
- [ ] Restart/recovery stress tests with large WALs.
- [ ] Concurrent player-update and read-heavy workload tests.
- [ ] Memory ceiling, tail latency, and queue-backpressure measurements.

## 7. Source layout

- [x] SQL parsing helpers moved to src/sql.rs.
- [x] Development server and its tests moved to src/server.rs.
- [x] WAL and fixed-page codec moved into src/storage/wal.rs and src/storage/pages.rs.
- [ ] Split storage into codec and allocator modules.
- [ ] Split the engine into catalog, index, execution, transaction, and buffer-pool modules.

## 8. Storage roadmap

- [ ] EmberTree page-oriented B+ tree and extent allocator.
- [ ] Explicit CoW and in-place update modes with crash-safe recovery.
- [ ] FragmentationStats and manually budgeted defrag APIs.
- [ ] Per-page LZ4/Snappy compression with decompression limits.
- [ ] Snapshot and incremental backup/restore system.
- [ ] Explicit page/index/query/WAL memory budgets.

## Release gate

Do not label PostemberDB production-ready until the page cache, transaction/recovery tests, network security, JDBC/KSL integration, backup/restore process, and repeatable performance tests are complete.
