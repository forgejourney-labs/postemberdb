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

- [x] Single-column typed B+ tree equality indexes backed by the engine-owned B+ tree implementation.
- [x] CREATE INDEX and SELECT equality predicates.
- [x] Index maintenance during INSERT and bounded bulk insert.
- [x] Persist index definitions and rebuild index entries after snapshot load.
- [x] Restore index definitions through WAL replay.
- [x] Unique composite primary-key indexes for SQL PRIMARY KEY constraints.
- [x] Native B+ tree node splitting, point lookup, inclusive in-memory range scan, and structural validation; SQL equality indexes now use this tree.
- [ ] User-defined general composite indexes, range scans, ordering, and a cost-aware index planner.
- [ ] Cost-aware query planner and EXPLAIN output.

## 3. Page-oriented storage

- [x] Fixed 4 KiB PPG1 snapshot pages with per-page and whole-payload checksums.
- [x] Strict PPG1 validation for canonical payload lengths, zero padding, and platform-safe length conversion.
- [x] Experimental file-backed EPG1 data pages with logical page IDs, generations, payload checksums, and canonical zero-padding validation.
- [x] Mirrored EAM3 allocator metadata with durable root-page publication; EAM2 metadata remains readable and is migrated on the next metadata commit, one damaged copy is repaired from the valid copy, and two invalid copies fail closed.
- [x] First-fit contiguous extent allocation, adjacent free-range coalescing, reuse, and reopen tests.
- [x] Backward-compatible reads for earlier raw PDB1/PDB2/PDB3 snapshots plus current PDB4 payloads inside PPG1 page files.
- [ ] Bounded page cache / buffer pool.
- [ ] Lazy page reads and streaming table scans.
- [ ] Page-level row updates instead of full snapshot rewrite at checkpoint.
- [x] In-memory page-ID allocator with lowest-ID reuse, high-water tracking, and double-free/unknown-ID rejection.
- [x] EmberTree page-backed CoW snapshots with immutable leaf/internal pages, redo WAL, and atomic root publication through EAM3 metadata.
- [x] `Database::open` detects existing EmberTree sidecars and replays a pending page-tree redo record before returning the database.
- [x] Stable, versioned binary codec for scalar/composite SQL index keys and bounded row-reference lists, with signed-integer byte ordering and corruption checks.
- [x] Persist namespaced SQL index entries and ordered row references in EmberTree after the canonical SQL WAL commit; rebuild the derived index tree from snapshot + replayed SQL WAL during open.
- [x] Route supported SQL equality lookups through exact-key persistent EmberTree scans, validate row references, and fall back to the in-memory index when a key cannot be represented by the current page-key limit.
- [x] Add bounded EmberTree range scans with subtree pruning and push SQL LIMIT into persistent equality-index scans.
- [x] Validate full reachable-tree structure on recovery, including separator/minimum agreement, ordered non-overlapping child ranges, cycles, and multiply referenced pages.
- [x] Harden redo WAL decoding against duplicate page IDs and malformed page images.
- [ ] Incremental path-copy insert/delete, cross-log crash fault injection, and old-page reclamation/vacuum.
- [x] Native in-memory B+ tree core with leaf-only values, internal separators, node splitting, point lookup, inclusive range scan, and structural validation.
- [x] In-memory B+ tree deletion with leaf/internal borrowing, merging, root collapse, and strict occupancy validation; focused and deterministic reference tests added.
- [x] Stack-based successor-leaf traversal for in-memory range scans.
- [ ] Persistent leaf sibling links (optional follow-up), page serialization, persistent index nodes, and catalog/page-file integration.

The native B+ tree now backs SQL index equality lookups and supports point lookup, insertion/replacement, deletion with sibling borrowing and merging, root collapse, structural validation, and in-memory range scans that advance between leaves using an ancestor stack. Focused tests include deterministic mixed operations checked against Rust's BTreeMap. A stable EIK1/EIR1 codec defines the disk representation for SQL index keys and row references, including ordered signed integers and strict bounded decoding. After each SQL WAL commit, the database rebuilds a namespaced persistent EmberTree index snapshot using one ordered tree entry per row reference, avoiding a single-page fanout ceiling for duplicate keys. On open, the SQL snapshot is loaded and the SQL WAL replayed first; the persistent index mirror is then rebuilt from that canonical state, so a missing or stale sidecar is repairable. Focused tests verify persistent index entries, a 700-row duplicate-key group, and rebuilding a deleted sidecar from SQL WAL. For representable keys, SQL equality queries now scan the persistent EmberTree and validate that returned row references are strictly ordered and in range; unsupported oversized keys safely disable the persistent mirror and use the in-memory B+ tree instead. The SQL WAL remains authoritative, while the persistent index is a rebuildable derived structure. Bounded range scans now stop at SQL LIMIT and prune internal subtrees outside the requested upper bound. Startup and redo recovery validate the complete reachable tree, including separators, child ranges, cycles and duplicate references; redo records reject duplicate page IDs and malformed page images before replay. A durable SQL commit remains successful if only the derived EmberTree mirror fails; the database disables that mirror and falls back to its in-memory index until the next successful rebuild. Incremental path-copy insert and delete APIs now exist at the page-tree layer. Delete batches merge adjacent same-level siblings when the combined page fits and collapse unary roots; redistribution of underfull siblings, cross-log crash fault injection and old-page reclamation remain pending. The tree does not yet use persistent leaf sibling pointers: nodes and index entries are still memory-resident, index definitions are persisted, and entries are rebuilt from rows on open. The PPG1 snapshot format remains a physical snapshot layout, not a lazy page store. A separate experimental ExtentFile primitive persists EPG1 data pages and mirrored EAM3 metadata, including the B+ tree root page. The page_tree module builds immutable B+ tree leaf/internal pages, writes a redo record, syncs pages, and publishes the root only after page writes; `Database::open` now invokes recovery when the `<database>.pages` or `<database>.pages.btree.wal` sidecar exists. This is now used as a durable derived mirror of SQL index entries, coordinated by rebuilding only after the canonical SQL WAL commit and after SQL WAL replay on open. It is not yet the source of truth for SQL rows/catalog or the SQL equality-query path. Each replacement rebuilds the complete tree and retains old pages; reclamation is pending. Database startup still reconstructs the PPG1 snapshot and loads all rows into memory.

## 4. KSL/JVM adapter

- [x] Typed positional SQL parameters in the Rust API.
- [x] Typed positional parameters inside transaction sessions.
- [x] SQL LIMIT clause applied during index lookup and table scans.
- [ ] TTL expiry index and explicitly budgeted expiration sweeps.
- [x] Bounded bulk insert API aligned with KSL's configurable batch ceiling.
- [x] Transaction callback API for Rust callers.
- [x] Versioned JSONL/1 request/response protocol with typed JSON values and legacy line-protocol compatibility.
- [ ] JDBC driver and service-provider registration.
- [x] Separate KSL addon service with loopback-only client, configuration, and async query/write methods.
- [ ] JDBC driver, native prepared statements, transaction scope in the addon, and KSL JDBC dialect/connection-pool integration.
- [ ] PreparedStatement, ResultSet, timeout, row-limit, and cancellation compatibility tests.
- [ ] End-to-end tests using the actual KSL plugin.

A separate PostemberDBKSL addon now registers a KSL service and exposes async query/write methods over JSONL/1. It is not a JDBC driver and does not replace KSL's existing JDBC backend. End-to-end testing against a running Paper/KSL server is still pending.

## 5. Security and operations

- [x] Development server rejects non-loopback bind addresses until TLS/authentication exist.
- [x] KSL bridge independently validates loopback destinations and refuses remote database hosts.
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
- [x] Split storage into page codec, WAL, and page allocator modules.
- [ ] Split the engine into catalog, index, execution, transaction, and buffer-pool modules.

## 8. Storage roadmap

- [ ] EmberTree page-oriented B+ tree (the in-memory B+ tree and an experimental persistent extent-file primitive exist; catalog/root integration and WAL-coordinated page commits remain pending).
- [ ] Explicit CoW and in-place update modes with crash-safe recovery.
- [ ] FragmentationStats and manually budgeted defrag APIs.
- [ ] Per-page LZ4/Snappy compression with decompression limits.
- [ ] Snapshot and incremental backup/restore system.
- [ ] Explicit page/index/query/WAL memory budgets.

## Release gate

Do not label PostemberDB production-ready until the page cache, transaction/recovery tests, network security, JDBC/KSL integration, backup/restore process, and repeatable performance tests are complete.


## 9. Incremental EmberTree writes

- [x] Copy-on-write single-key insert/replace API for the persistent page tree.
- [x] Batch insert/replace API with ordered last-write-wins semantics for duplicate keys.
- [x] Recursive leaf/internal path copying and leaf/internal page splitting.
- [x] One redo record and one root publication for an insert batch.
- [x] Preserve untouched child page IDs when a path is updated.
- [x] Reopen/recovery tests for incremental writes and root publication.
- [x] Incremental copy-on-write deletion batches with sibling merge when pages fit and root collapse.
- [ ] Redistribution/rebalancing of underfull siblings when two pages cannot be merged.
- [x] SQL transaction delta integration for the derived EmberTree index mirror: committed transactions remove obsolete row-reference keys and insert only new keys instead of rebuilding all pages.
- [ ] Avoid full in-memory index scans when computing deltas; current diff collection is O(total indexed entries), and row-position shifts after DELETE can produce many changed references.
- [ ] Reachability-aware reclamation and vacuum; old CoW pages remain allocated.
- [ ] Fault injection at allocation, redo sync, page write, and root publication boundaries.

The incremental insert/delete APIs are now used by SQL transaction commits for the derived index mirror.  Delete batches ignore missing keys, publish one redo record/root per batch, merge adjacent same-level siblings when their combined contents fit one page, and collapse unary roots. Non-root unary internal pages may remain when siblings cannot be merged; byte-balanced redistribution and a minimum-occupancy invariant are not implemented. SQL transaction commits now compute the set difference between the previous and next derived index entries, then apply only removed/inserted keys through the incremental page-tree APIs. This reduces persistent page writes and preserves untouched CoW subtrees. The current delta collector still scans all in-memory index entries to calculate the difference (O(total indexed entries)); positional row references also mean DELETE may shift many references. SQL table rows are not page-backed. Allocation metadata is committed as new pages are reserved; an interrupted operation before its redo record can leave unreachable allocated pages. This is safe for correctness but requires future vacuum/reclamation support.
