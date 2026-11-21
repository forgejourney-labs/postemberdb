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
- [x] Explicit SQL `BEGIN`/`COMMIT`/`ROLLBACK` in the development server.
- [ ] Savepoints and transaction isolation levels.
- [x] OS-level exclusive sidecar lock for the same database path.
- [ ] Hard-link alias detection and broader multi-writer coordination.
- [ ] Fault-injection tests for power loss at every write/sync/rename boundary.

## 2. Indexes

- [x] Single-column typed B+ tree equality indexes backed by the engine-owned B+ tree implementation.
- [x] CREATE INDEX and SELECT equality predicates.
- [x] Index maintenance during INSERT and bounded bulk insert.
- [x] In-memory SQL index entries store stable table-local row IDs and resolve them through the row-position map.
- [x] KeyValueStore supports bounded prefix deletion in atomic batches of up to 5,000 keys, avoiding full-range materialization for repeated cleanup.
- [x] Incremental index-key maintenance for UPDATE, DELETE, and UPSERT; deleting rows no longer rebuilds index trees.
- [x] Unique-index collision validation for multi-row UPDATE before staged rows are mutated.
- [x] Persist index definitions and rebuild index entries after snapshot load.
- [x] Restore index definitions through WAL replay.
- [x] Unique composite primary-key indexes for SQL PRIMARY KEY constraints.
- [x] Native B+ tree node splitting, point lookup, inclusive in-memory range scan, and structural validation; SQL equality indexes now use this tree.
- [x] SELECT ORDER BY ASC/DESC with multiple sort keys; disk-backed PDB6 scans sort matching result rows.
- [x] WHERE predicates support equality/inequality and ordered comparisons, `IS NULL`, `IS NOT NULL`, null-safe `IS DISTINCT FROM` / `IS NOT DISTINCT FROM`, case-sensitive `LIKE` / `NOT LIKE` with `%` / `_` wildcards, `IN` / `NOT IN`, and inclusive `BETWEEN` / `NOT BETWEEN`; top-level `AND` / `OR` use SQL precedence (`AND` before `OR`). SQL comparisons to NULL do not match rows; `IN` / `NOT IN` account for NULL list members.
- [x] Recursive `WHERE` expression trees with nested parentheses, unary `NOT`, and SQL three-valued `AND` / `OR` / `NOT` evaluation (`TRUE`, `FALSE`, `UNKNOWN`); the same evaluator is used by SELECT, UPDATE, DELETE, and disk-backed PDB6 scans.
- [ ] Full SQL expression grammar, scalar expressions, CASE, casts, collations, and query-planner normalization remain unfinished.
- [ ] User-defined general composite indexes, indexed range scans, and a cost-aware index planner.
- [ ] Cost-aware query planner and EXPLAIN output.

## 3. Page-oriented storage

- [x] Fixed 4 KiB PPG1 snapshot pages with per-page and whole-payload checksums; current PDB5 payloads persist stable table-local row identifiers and their monotonic next-ID counters, while PDB4 and older formats remain readable.
- [x] Strict PPG1 validation for canonical payload lengths, zero padding, and platform-safe length conversion.
- [x] Experimental file-backed EPG1 data pages with logical page IDs, generations, payload checksums, and canonical zero-padding validation.
- [x] Variable-length RecordStore prototype: records up to 64 MiB span contiguous EPG1 pages, RDX1 descriptors are indexed by stable caller IDs in the EmberTree B+ Tree, and page/whole-record integrity is checked on reads.
- [x] RecordStore put_many/delete_many batches validate before allocation and publish descriptor changes through one page-tree redo record/root update (up to 5,000 IDs and 256 MiB of unique record data per put batch).
- [x] RecordStore provides bounded ordered ID pagination without reading payloads, plus record-page reads capped at 5,000 records and 256 MiB per call.
- [x] Standalone `SqlRowStore` stores typed SRW1 SQL row records over RecordStore pages, with stable-ID lookup, bounded scans, schema-width checks, and corruption validation; it is not yet integrated into `Database`.
- [x] RecordStore and SqlRowStore expose mixed upsert/delete batches committed through one descriptor-tree redo frame and one atomic root publication; this is a storage primitive, not yet the SQL transaction coordinator.
- [x] SqlRowStore bounded multi-get by stable row ID, preserving first-seen order, deduplicating IDs, and enforcing 5,000-ID / 256 MiB read limits; this remains a row-store API, not SQL executor integration.
- [x] PDB6 is the canonical SQL authority after explicit `Database::migrate_to_pdb6()`; `Database::open` loads a published PDB6 manifest first. Unmigrated legacy databases remain PDB5-backed and readable.
- [x] PDB6 PCT1 catalog codec and versioned key constructors are used by the live PDB6 SQL migration/load/commit path.
- [x] PDB6 PDM1 manifest is published by the SQL migration/commit path and validated during `Database::open`.
- [x] Initial shared Pdb6Store over one ExtentFile/EmberTree root, with keyspace validation, atomic mixed mutation batches, bounded prefix scans, manifest access, and reopen tests.
- [x] PDB6 P6R1 overflow row pages + P6D1 descriptors are used by SQL migration/load/commit, with checksummed reads, 64 MiB per-row / 256 MiB per-batch bounds, keyset scans, and shared CoW root publication for row/catalog/index mutations.
- [x] Explicit `Database::create_pdb6` API and `create-pdb6` CLI create new PDB6 databases without overwriting existing files; new server databases initialize as PDB6, while existing PDB5 files remain compatible.
- [x] PDB6 open loads catalog metadata and row counts without hydrating row payloads; SELECT and table-page reads stream validated rows from disk in bounded batches.
- [x] PDB6 LIMIT stops disk reads early when ordering is not requested; COUNT(*) counts by streaming and does not materialize row payloads in its result.
- [x] Committed PDB6 DML evicts temporary row caches; write transactions hydrate only the existing table addressed by each write, leaving unrelated tables disk-backed.
- [x] The fixed-page cache uses an indexed intrusive LRU: cache hits and recency updates are O(1), while ordered page-range invalidation remains supported.
- [x] Supported plain INSERT and single-column UPDATE/DELETE statements stage changes without hydrating the complete affected table; UPDATE/DELETE enforce both a 5,000-row ceiling and a 128 MiB retained-row-value budget.
- [ ] Shared bounded SQL row cache and page pinning across SQL operations remain unfinished.
- [x] Mirrored EAM3 allocator metadata with durable root-page publication; EAM2 metadata remains readable and is migrated on the next metadata commit, one damaged copy is repaired from the valid copy, and two invalid copies fail closed.
- [x] First-fit contiguous extent allocation, adjacent free-range coalescing, reuse, and reopen tests.
- [x] Backward-compatible reads for earlier raw PDB1/PDB2/PDB3 snapshots plus current PDB4 payloads inside PPG1 page files.
- [x] Initial bounded LRU cache for validated EPG1 extent-page payloads, with per-handle capacity limits, write-through updates, release/reuse invalidation, and hit/miss/eviction statistics; `KeyValueStore` exposes cache configuration and metrics.
- [ ] Pin-aware shared SQL buffer pool, database-wide cache budgeting, and bounded write staging.
- [ ] Page-level in-place SQL row updates, safe overflow-page reuse, and incremental backup integration.
- [x] In-memory page-ID allocator with lowest-ID reuse, high-water tracking, and double-free/unknown-ID rejection.
- [x] EmberTree page-backed CoW snapshots with immutable leaf/internal pages, redo WAL, and atomic root publication through EAM3 metadata.
- [x] `Database::open` detects existing EmberTree sidecars and replays a pending page-tree redo record before returning the database.
- [x] Stable, versioned binary codec for scalar/composite SQL index keys and bounded row-reference lists, with signed-integer byte ordering and corruption checks.
- [x] Persist namespaced SQL index entries and ordered stable row identifiers in EmberTree after the canonical SQL WAL commit; rebuild the derived index tree from snapshot + replayed SQL WAL during open.
- [x] Route supported SQL equality lookups through exact-key persistent EmberTree scans, validate stable row identifiers and resolve them through a table-local ID-to-position map, and fall back to the in-memory index when a key cannot be represented by the current page-key limit.
- [x] Add bounded EmberTree range scans with subtree pruning and push SQL LIMIT into persistent equality-index scans.
- [x] Validate full reachable-tree structure on recovery, including separator/minimum agreement, ordered non-overlapping child ranges, cycles, and multiply referenced pages.
- [x] Harden redo WAL decoding against duplicate page IDs and malformed page images.
- [x] Incremental path-copy insert/delete and conservative tree-node plus descriptor-aware overflow-page reclamation; [ ] cross-log crash fault injection and snapshot-/reader-aware reclamation.
- [x] Native in-memory B+ tree core with leaf-only values, internal separators, node splitting, point lookup, inclusive range scan, and structural validation.
- [x] In-memory B+ tree deletion with leaf/internal borrowing, merging, root collapse, and strict occupancy validation; focused and deterministic reference tests added.
- [x] Stack-based successor-leaf traversal for in-memory range scans.
- [ ] Persistent leaf sibling links (optional follow-up), page serialization, persistent index nodes, and catalog/page-file integration.

The native in-memory B+ tree backs SQL equality indexes and supports point lookup, insertion/replacement, deletion with sibling borrowing and merging, root collapse, structural validation, and range scans. A stable EIK1/EIR1 codec defines the disk representation for SQL index keys. The persistent EmberTree sidecar stores one namespaced entry per indexed row using stable table-local row IDs. Supported SQL equality queries read this persistent mirror when valid, resolve row IDs through a table-local position map, and validate references; unsupported oversized keys or a failed mirror safely fall back to the in-memory index. The SQL snapshot/WAL remains authoritative for table rows and catalog metadata, and the persistent index mirror is rebuilt from canonical SQL state on open.

SQL transaction commits no longer diff complete previous/current index snapshots. INSERT, UPDATE, DELETE, UPSERT, and CREATE INDEX now append encoded row-level index mutations while executing against the staged transaction. Repeated changes to the same key are coalesced in transaction order (last mutation wins), then removals and upserts are applied through one mixed CoW batch, one redo record, and one root publication after the canonical SQL WAL commit. This avoids O(total indexed entries) transaction-time delta discovery. A full scan remains necessary when rebuilding the derived mirror at startup. In-memory SQL indexes now store stable table-local row IDs rather than vector positions. INSERT adds only the new IDs, UPDATE removes and reinserts an ID only when an indexed key changes, DELETE removes each deleted row from its index keys, and UPSERT rekeys only the affected row. A DELETE still compacts the row vector and rebuilds the row-ID-to-position map in O(remaining rows), but it does not rebuild the B+ tree indexes. Unique-index updates are checked as a group before any staged row is changed.

Startup and redo recovery validate the complete reachable persistent tree, including separators, child ranges, cycles, and duplicate references; redo records reject duplicate page IDs and malformed page images before replay. PDB6 startup loads catalog metadata and row counts only; SQL SELECT and table-page reads stream row payloads directly from PDB6 pages. Plain single-statement INSERT/UPDATE/DELETE through `Database::execute` scan row IDs in 512-entry batches, retain only changed rows (maximum 5,000 subject to the aggregate mutation budget), validate unique-index conflicts against persistent index keys, and publish row descriptors, index deltas, catalog counters, and the manifest through one CoW root. PDB6 `insert_many` also writes bounded row batches directly without hydrating the table. UPDATE/DELETE still scan the table when selecting rows, but no longer materialize the whole table. Multi-statement transactions, DatabaseSession, and UPSERT retain the compatibility hydration path for now. The shared bounded SQL buffer pool and page pinning remain pending. A durable SQL commit remains successful if only the derived PDB5 EmberTree index mirror fails; the database disables that mirror and falls back to in-memory indexes until a successful rebuild. Incremental page-tree insertion/deletion uses path copying; deletion merges adjacent same-level siblings when their combined contents fit and collapses unary roots. Vacuum validates every live P6D1 row descriptor and its P6R1 page headers, preserves referenced overflow extents, and reclaims orphaned P6R1 pages plus unreachable tree nodes; unknown page types are retained. Redistribution of underfull siblings, cross-log crash fault injection, and snapshot/reader-pin protection remain pending. The persistent tree does not yet use leaf sibling pointers. The PPG1 snapshot remains a legacy physical snapshot layout; PDB6 uses page-backed canonical row storage.
## 3.1 EmberForge 7 (EF7) — Minecraft/KSL record engine

- [x] Independent EF7 sidecar and versioned EF7C overflow chunks / EF7R record descriptors over the PDB6 page allocator and CoW B+ tree.
- [x] Checksummed multi-page records up to 64 MiB with bounded page counts and whole-record verification.
- [x] Hierarchical namespace metadata, parent-scoped unique names, bounded listing, and non-cascading empty-only deletion.
- [x] Raw player records with persistent UUID and row-identity indexes; stale UUID mappings are removed when a row changes UUID.
- [x] Typed Minecraft player profiles with username, first/last-seen timestamps, playtime ticks, and plugin-owned custom bytes.
- [x] Atomic typed profile batches with UUID + username index maintenance in one CoW root publication.
- [x] Namespace-scoped, case-insensitive player-name index; bounded UUID pagination and name-prefix search.
- [x] Signed-coordinate Minecraft chunk storage with ordered X/Z key encoding, bounded rectangular scans, and atomic region chunk batches.
- [x] Atomic player batch updates/removals with one CoW redo/root publication and post-publication payload reclamation.
- [ ] General user-defined secondary indexes and index rebuild/verification tooling.
- [ ] Enlist EF7 mutations in the same user-visible SQL transaction and coordinator commit path; EF7 remains a separate durable participant.
- [x] KSL JSONL/1 typed player-profile bridge: profile put/get by UUID or username, delete, and atomic profile batches; asynchronous Kotlin client methods are implemented. Custom-data transport is capped at 24 KiB by the line protocol.
- [x] KSL JSONL/1 namespace administration: create/get/find/list/delete with parent-scoped hierarchy; bridge listing is capped at 64 entries per call.
- [x] KSL JSONL/1 chunk CRUD with signed coordinates and a pre-read descriptor-size check; the bridge caps chunk payloads at 24 KiB.
- [x] KSL JSONL/1 generic record CRUD and explicit TTL record expiry/sweep methods; record payloads are capped at 24 KiB.
- [x] Initial Binary/1 24-byte frame codec with version/kind/flags/request ID/length/CRC32, bounded 8 MiB frames, strict validation, and clean-EOF versus truncated-frame handling.
- [x] Loopback server Binary/1 handshake, ping/pong, SQL statement frames, and single-frame EF7 chunk put/get/delete (8 MiB per-frame ceiling).
- [ ] Multi-frame chunk streaming, aggregate in-flight byte budgets, backpressure, cancellation, range scans, atomic chunk batches, and Kotlin binary client.
- [ ] Binary/1 transaction-control frames and cross-domain transaction coordination.
- [ ] Build the addon against the installed KSL API and Paper dependencies; run end-to-end Paper/KSL integration tests.
- [ ] JDBC driver and KSL SQL dialect/PreparedStatement support.
- [ ] Crash-point fault injection, extended reopen/corruption tests, and production-like Minecraft/KSL load tests.

EF7 owns `.ef7.pages`, `.ef7.wal`, and `.ef7.lock`. It remains an independent record engine and does not yet provide cross-domain atomicity with SQL, KV, Registry, or other sidecars. FNV-style checksums detect accidental corruption; they are not cryptographic authentication.

## 4. KSL/JVM adapter

- [x] Typed positional SQL parameters in the Rust API.
- [x] Typed positional parameters inside transaction sessions.
- [x] SQL LIMIT clause applied during index lookup and table scans.
- [x] EF7 generic-record TTL index ordered by expiry/namespace/record, atomic expiry rekeying, bounded 1..=10,000 cleanup batches, descriptor-limited reads, and post-publication extent reclamation.
- [x] KSL JSONL/1 record put/get/delete and explicit TTL sweep methods with a 24 KiB payload cap.
- [ ] TTL expiry index for SQL/KV/Registry and automatic scheduler integration.
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
- [x] Argon2id user authentication, persisted permission bitsets, enabled/disabled users, password rotation, named roles with durable membership and permission inheritance, and checksummed audit events for login outcomes and user/role administration; user/role mutations and their audit records share one atomic page-tree batch.
- [x] `AuthStore::as_admin` requires an authenticated `Principal` with `Admin` and attributes all administrative audit events emitted by its closure to that principal; nested actor contexts are rejected.
- [ ] Audit retention/export policy, audit tamper-evidence beyond per-record checksums, and per-database/schema/table grants.
- [ ] Mutual TLS and per-user rate/connection quotas.
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
- [x] Direct row-level SQL index journaling for INSERT, UPDATE, DELETE, UPSERT, and CREATE INDEX.
- [x] Coalesce repeated row-key mutations in transaction order and publish one mixed CoW batch after the canonical SQL WAL commit.
- [x] Stable row IDs in persistent index entries prevent row-position shifts after DELETE.
- [ ] Replace in-memory index row positions with stable row IDs and remove full in-memory index rebuilds on UPDATE/DELETE.
- [x] Conservative PDB6 vacuum traces the current published B+ Tree root and frees unreachable EBL1/EBI1 nodes while retaining P6R1 overflow and unknown page types.
- [ ] Descriptor-aware reclamation of orphan overflow extents, named snapshot roots, active-reader pins/epochs, and crash-safe vacuum progress.
- [x] Test-only B+ tree crash injection after redo sync, after the first page sync, after all page syncs, and after root publication; recovery tests also cover interrupted deletion of the final key and restoration of an empty root.
- [x] Test-only allocator metadata fault injection after the primary durable slot sync and after the mirrored slot sync; reopen tests verify that committed reservations are never reissued.
- [x] Test-only recovery coverage for a durably torn primary metadata slot and a crash after file extension but before high-water-mark publication.
- [x] Deterministic allocator-churn test checks exact allocated-page ownership through repeated reserve/release cycles and periodic reopen; metadata generation zero is rejected.
- [ ] Fault injection for device-level write reordering/flush guarantees and cross-log transaction boundaries.

The incremental insert/delete APIs are now used by SQL transaction commits for the derived index mirror.  Delete batches ignore missing keys, publish one redo record/root per batch, merge adjacent same-level siblings when their combined contents fit one page, and collapse unary roots. Non-root unary internal pages may remain when siblings cannot be merged; byte-balanced redistribution and a minimum-occupancy invariant are not implemented. SQL transaction commits now compute the set difference between the previous and next derived index entries, then apply removed and inserted keys through one mixed page-tree mutation batch, one redo record, and one root publication. This reduces persistent page writes and preserves untouched CoW subtrees. Stable table-local row identifiers are now stored in PDB5 snapshots and used in EmberTree index keys, so deleting a row does not renumber later entries. PDB4 and earlier snapshots remain readable; legacy rows receive deterministic IDs when loaded. The current delta collector still scans all in-memory index entries to calculate the difference (O(total indexed entries)). SQL table rows are not page-backed. Allocation metadata is committed as new pages are reserved; an interrupted operation before its redo record can leave unreachable allocated pages. This is safe for correctness but requires future vacuum/reclamation support.


## Native Key-Value and Registry storage

Implemented a standalone `KeyValueStore` backed by the native page-backed CoW B+ Tree. It supports arbitrary byte keys, byte values that fit alongside their keys in one leaf entry, exact get/put/delete, batched upserts, mixed mutation batches, ordered prefix scans, and prefix deletion. Each batch is one redo-WAL transaction and one root publication. The store uses separate `.pages`, `.pages.wal`, and exclusive `.lock` sidecars.

Implemented a typed `Registry` layered on that KV engine. It provides top-level hives, nested keys, case-insensitive slash/backslash paths, named values (including the default empty-name value), enumeration, and safe non-recursive or recursive key deletion. Supported value types are String, Integer, Boolean, Binary, and JSON text. JSON text is not parsed or schema-validated by the database.

File-backed `Database` instances can open their independent `open_key_value_store()` and `open_registry()` sidecars. These stores are not yet enlisted in the SQL transaction WAL, so atomic commits spanning SQL + KV + Registry are not supported. Large values requiring overflow/blob pages, per-hive ACL/RBAC, encryption-at-rest, and audit events remain future work.


## Explicit PDB5-to-PDB6 migration foundation

- [x] Added `Database::migrate_to_pdb6()`: copies catalog definitions, typed SRW1 row payloads, stable row IDs, and index entries into the PDB6 shared root.
- [x] Publishes the PDB6 manifest only after the copy is complete; PDB5 snapshot/WAL are not modified by the migration.
- [x] Bounded row and index batches to 5,000 mutations.
- [x] Read-back verification of catalog records, every migrated row payload, every index key, and catalog cardinality before publishing the manifest.
- [x] PDB6 is the live SQL authority after explicit migration; PDB5-only databases remain readable and continue using PDB5 until `postemberdb migrate-pdb6 <data-file>` is run.
- [ ] Automatic resume/cleanup of partially copied overflow pages and full end-to-end migration verification remain pending; interrupted copies require operator removal of the incomplete PDB6 sidecars before retry.


## Cross-domain Transaction Coordinator foundation

- [x] Durable coordinator decision journal with monotonic transaction IDs, bounded participant sets, fsync'd BEGIN/PREPARED/COMMIT/ABORT records, and CRC validation.
- [x] Recovery reports undecided transactions and truncates only a torn final record; complete corrupt records fail closed.
- [x] COMMIT is rejected until every participant has a durable PREPARED marker.
- [x] KV, Registry, and EF7 now have durable prepared-participant adapters with idempotent committed replay and APPLIED acknowledgements.
- [ ] SQL still needs a durable participant adapter. Full SQL + KV + Registry + EF7 atomicity is not available yet.


## Coordinator participant-application recovery

- [x] Coordinator journal now records durable per-participant APPLIED acknowledgements after COMMIT.
- [x] Recovery distinguishes committed transactions that still need participant application from transactions whose participants have all acknowledged.
- [x] APPLIED records are checksummed, ordered after COMMIT, and reject duplicate or unprepared participant IDs.
- [x] KV, Registry, and EF7 are enlisted through durable prepare markers and idempotent commit replay; coordinator APPLIED is synced before prepare-marker cleanup.
- [ ] SQL adapter and durable abort-acknowledgement recovery remain pending; the system does not yet guarantee cross-domain atomicity across all four domains.


## Live PDB6 SQL authority

- [x] A complete PDB6 manifest is now detected on open and takes precedence over the retained PDB5 snapshot/WAL.
- [x] SQL transactions publish changed row payloads, catalog records, index deltas, and the updated manifest in one PDB6 root transaction.
- [x] PDB6-backed SQL handles no longer append to or rewrite the PDB5 snapshot/WAL; PDB5 files remain intact as a compatibility source.
- [x] Reopen/DML regression test covers insert, update, delete, unique index lookup, and persistence after another reopen.
- [x] SQL open, SELECT, and table-page reads remain metadata-only and use bounded streaming disk scans; single-statement DML has a bounded page-native write path.
- [ ] Multi-statement SQL transactions and prepared SQL commits still use compatibility hydration; persistent index range planning and DML predicate pushdown remain pending.
- [ ] PDB6 index/catalog transaction crash-point injection and automatic cleanup of unreachable overflow pages remain pending.


## Prepared domain participants

- [x] KV supports durable, restart-discoverable prepared mutation batches; direct writes are blocked while a prepare is unresolved.
- [x] Registry can prepare value updates/deletions and participates through its own stable coordinator ID.
- [x] EF7 can prepare raw keyspace batches and typed chunk replacements/deletions; payload pages are synced before descriptor preparation and old extents are not released by the prepared path.
- [x] Coordinator replays committed participants idempotently, syncs APPLIED, then removes each participant prepare marker; crash retry is covered for KV and EF7, with Registry value application covered by tests.
- [x] SQL, KV, Registry, and EF7 expose durable coordinator participant adapters; the coordinator verifies durable prepares, records a commit/abort decision, and persists APPLIED/FINALIZED/ROLLED_BACK acknowledgements.
- [x] Server startup opens all four participants and replays committed/aborted coordinator decisions before binding its listener; PREPARING transactions block startup.
- [x] Durable abort acknowledgements and idempotent abort recovery across prepared participants.
- [ ] Snapshot-/reader-aware reclamation and crash recovery across every storage/allocator boundary remain pending.


## TLS, authentication, RBAC and JDBC baseline

- [x] TLS listener using rustls; non-loopback binds are refused unless TLS mode is enabled.
- [x] Argon2id password hashes, user enable/disable, role permission sets, and hidden-terminal-password user provisioning.
- [x] SQL and EF7 read/write authorization on JSONL and supported binary operations; unknown command classes fail closed.
- [x] Standalone JDBC Maven module with Connection, Statement, PreparedStatement, ResultSet, metadata basics, TLS and JSONL/1 transport.
- [ ] Server-side prepared-statement protocol, streaming ResultSets, JDBC batch/generated keys/savepoints, and complete JDBC metadata remain pending.
- [x] Durable password rotation with independent Argon2 salt/hash persistence.
- [x] Durable authentication audit events are stored in the auth sidecar with monotonic sequence IDs, checksums, bounded page reads, and no password material.
- [x] Actor-attributed administrative audit identity through `AuthStore::as_admin`; legacy direct local administration remains explicitly attributed as `local-api`.
- [ ] Audit retention/export, mutual TLS, per-user rate/connection quotas, and schema/table-scoped grants remain pending.

## Vacuum and snapshots

- [x] Current-root and named-snapshot-root reachability for B+ Tree nodes; durable `PSN1` snapshot registry uses checksummed records and atomic temp-file replacement with file and parent-directory sync.
- [x] `Database::{create_pdb6_snapshot, list_pdb6_snapshots, delete_pdb6_snapshot}` manage persistent named roots. Vacuum preserves tree pages reachable from every retained root.
- [x] Descriptor-aware overflow-page reachability for the current root. While any named snapshot exists, Vacuum conservatively retains every P6R1 overflow page because old snapshot descriptors are not yet independently enumerated; after the final snapshot is deleted, orphan overflow pages can be reclaimed.
- [ ] Active reader epochs/pins, per-snapshot overflow descriptor tracing (to reclaim safe orphans while snapshots exist), crash-safe vacuum progress, and subprocess fault injection across allocator release remain pending. PDB6's exclusive file lock currently serializes page-store access; concurrent shared readers are not supported.


- [x] Coordinator ABORT recovery now retries each prepared participant's idempotent rollback, syncs a per-participant ROLLED_BACK acknowledgement, and exposes aborted transactions still needing cleanup.
