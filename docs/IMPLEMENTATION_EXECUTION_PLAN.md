# Storage, transactions, JDBC, security and reliability execution plan

**Status:** engineering plan; none of the gates below are considered complete merely because this document exists.

This plan records the implementation order and invariants required before PostemberDB can claim disk-backed SQL rows, cross-subsystem atomicity, or safe remote server access.

## Current baseline (dev)

The current implementation has useful building blocks, but they are not yet one database transaction domain:

- Database owns PDB5 snapshot/WAL state and reconstructs Table.rows into memory.
- SqlRowStore is a typed SRW1 layer over RecordStore; it is not connected to the live SQL executor.
- Pdb6Store provides a shared CoW tree, redo WAL and catalog/row/index keyspaces, but the live SQL engine does not use it as its canonical store.
- KV, Registry, RecordStore and EF7 have independent sidecar files, roots and WALs.
- page_tree retains replaced/deleted pages. It does not yet perform complete root-reachability vacuum and safe page reuse.
- The server intentionally accepts loopback binds only. TLS, identity and authorization are absent.
- The repository has no JDBC driver module yet.

PDB5 must remain readable. A migration must never silently replace or discard the only valid copy of a database.

## Non-negotiable storage invariants

1. **One committed truth:** in PDB6 mode, catalog metadata, row descriptors, index definitions/entries and transaction counters are read from one canonical logical root. Derived caches may be rebuilt; they are never authoritative.
2. **Stable identity:** table IDs and row IDs are monotonic, non-zero identifiers. Physical page IDs and vector positions are never exposed as row identity.
3. **Bounded memory:** opening a database must not deserialize every row. Point reads fetch only required pages; scans use bounded batches/cursors. Cache capacity is explicit and observable.
4. **Commit ordering:** durable intent/redo data is synced before a root or commit marker makes it visible. Recovery is idempotent.
5. **No premature reuse:** a page cannot be returned to the allocator while reachable from the current root, retained snapshot, in-flight transaction or recovery record.
6. **Fail closed:** checksum, format, authentication, authorization and recovery errors must not be converted into success or partial visibility.
7. **Compatibility:** PDB5/PDB4 and documented legacy formats remain readable. Migration is explicit, restartable, verified and preserves the source until validation succeeds.

## Workstream A — canonical disk-backed SQL RowStore

### Design

- Promote PDB6 to canonical SQL persistence only after a complete reader/writer exists.
- Persist a versioned catalog record per table, stable row descriptors, index definitions and index entries in the shared PDB6 keyspace.
- Use overflow records for row payloads that do not fit in a B+ tree leaf value. A row descriptor points to the checked multi-page payload.
- Keep Table metadata and indexes in memory only as bounded/rebuildable metadata where justified. Do not keep Vec<Row> as authoritative table contents.
- Replace vector-position access with a row accessor resolving stable row IDs through the disk store/cache.
- Implement bounded SELECT iteration. LIMIT must stop reading pages once satisfied; cursors must not materialize the whole result.
- Update INSERT/UPDATE/DELETE and index maintenance to produce one logical mutation batch. Catalog, row descriptors, index entries and counters publish together.
- Preserve the PDB5 reader. Add an explicit PDB5-to-PDB6 migration command with a temporary destination, progress checkpoint, checksum/count verification and atomic final rename.

### Acceptance gates

- Reopen a database containing millions of rows without loading all row payloads into memory.
- Point lookup reads only the index/search path and selected row pages (subject to cache hits).
- A bounded scan has a documented memory ceiling and does not allocate a Vec<Row> for the complete table.
- Crash at each write/sync/root-publication boundary; recovery yields the complete old or complete new state, never mixed catalog/row/index state.
- Migration can be interrupted and resumed; source remains usable until final verification and promotion.
- Tests cover NULLs, current scalar types, maximum row size, overflow pages, duplicate keys, unique indexes, stable IDs and corruption.

## Workstream B — one transaction coordinator for SQL, KV, Registry and EF7

### Design

A single B+ tree root cannot atomically publish independent sidecar files. Do not claim cross-domain atomicity by calling four existing batch APIs in sequence.

Introduce a database-wide transaction coordinator with:
- monotonically increasing transaction IDs;
- a durable coordinator journal containing transaction identity, participant set, format version and state;
- participant prepare records with checksums and idempotency keys;
- a durable commit decision written only after every participant has prepared and synced;
- recovery that completes committed transactions and rolls back or discards transactions without a commit decision;
- visibility rules so readers cannot observe prepared-but-uncommitted participant state;
- bounded participant batches and explicit limits for total transaction bytes/mutations.

Each participant (SQL/PDB6, KV, Registry, EF7) must expose an internal prepare/apply/recover contract. Public APIs must not bypass the coordinator when participating in a multi-domain transaction. Standalone operations can remain standalone transactions.

### Acceptance gates

- Inject a crash after every prepare, sync, commit-decision and participant-apply boundary.
- After restart, all participants report the same transaction outcome.
- Replaying recovery twice produces the same state as replaying it once.
- Concurrent readers never see a committed transaction partially applied across domains.
- Conflicts, duplicate participant enlistment, missing sidecars and corrupted coordinator records fail deterministically and safely.

## Workstream C — CoW vacuum and page reclamation

### Design

- Enumerate every live root: current database root, retained snapshots, active reader pins, active writer roots and recovery/WAL roots.
- Mark all reachable B+ tree pages and data/overflow extents reachable through live descriptors.
- Build a candidate free set by comparing allocated pages with the complete mark set; never infer liveness from page age alone.
- Persist vacuum progress so interruption is restartable. A partially completed vacuum must not free pages prematurely.
- Reclaim only after a durable safe-point/epoch proves no reader or recovery path can reference the old page.
- Coalesce free extents, track fragmentation and expose dry-run statistics before destructive reclamation.
- Keep vacuum incremental with page/time budgets; do not hold a global writer lock for an unbounded full-file pass.

### Acceptance gates

- Randomized insert/update/delete workloads followed by vacuum preserve every live key and record.
- Snapshots and long-lived readers retain their pages until released.
- Repeated vacuum is idempotent and converges.
- Crash during mark, sweep, free-list publication and allocator metadata update never causes a live page to be reallocated.
- Reuse tests verify generation/checksum checks reject stale page references.

## Workstream D — TLS, authentication, RBAC and audit

### Design

- Keep loopback-only binding as the default until secure remote mode is fully configured.
- Add TLS with certificate/key validation, configurable minimum protocol version and explicit certificate rotation behavior.
- Add an authentication handshake before accepting SQL or EF7 operations. Password verifiers use a modern password-hashing KDF with per-user salts; plaintext passwords are never persisted or logged.
- Define principals, roles and permissions for database, schema, table, SQL operation, KV namespace, Registry path and EF7 namespace.
- Enforce authorization in the server dispatcher and again at the storage/service boundary where bypass is possible.
- Use deny-by-default permissions. Administrative operations require explicit privileges.
- Write append-only audit events for authentication outcomes, privilege changes, schema changes, data mutations and administrative operations. Do not log secrets or raw credentials.
- Bound handshake sizes, authentication attempts, connection counts, request sizes and timeouts. Add rate limiting and safe error responses.

### Acceptance gates

- No non-loopback bind without complete TLS, authentication and authorization configuration.
- Invalid, expired and untrusted certificates are rejected.
- Tests prove every role/permission boundary, including attempts to bypass SQL checks through KV, Registry and EF7 endpoints.
- Secrets are absent from logs, error strings and audit payloads.
- Credential rotation/revocation and account lockout behavior are tested.

## Workstream E — JDBC driver

### Design

Create a separately versioned Java driver module using only the stable, documented PostemberDB wire protocol.

Required JDBC surface:
- Driver registration and URL parsing;
- Connection, Statement, PreparedStatement, ResultSet and metadata;
- typed parameter binding without SQL string interpolation;
- transaction controls (autoCommit, commit, rollback, isolation reporting);
- generated keys only if the server protocol supports them;
- query timeout, socket timeout, cancellation and clean resource closure;
- SQLState/vendor error mapping;
- TLS and authentication configuration shared with the server;
- protocol capability/version negotiation.

The protocol must support parameterized queries and streamed result batches before the driver is called production-ready. Do not implement JDBC by sending arbitrary SQL strings over the legacy line protocol.

### Acceptance gates

- JDBC compatibility suite and relevant TCK behavior.
- Prepared statement tests for every supported type, NULL and injection-shaped values.
- Large result sets stream without loading all rows into the client or server heap.
- Transaction, timeout, cancellation, disconnect and reconnect tests.
- Integration tests against the actual Rust server in CI.

## Workstream F — fault injection, stress and performance

### Test infrastructure

- Add deterministic failpoints around file creation, writes, flushes, sync_all, rename, root publication, WAL truncation and allocator updates.
- Use subprocess crash tests so destructors and orderly shutdown do not mask recovery bugs.
- Add property/model tests comparing SQL and storage operations against a simple reference model.
- Add randomized concurrent reader/writer and multi-domain transaction tests.
- Add corruption tests for truncated headers, invalid lengths, checksum mismatches, duplicate IDs, invalid roots and malformed protocol frames.
- Run sustained workloads with bounded memory and report p50/p95/p99 latency, throughput, RSS, page-cache hit ratio, WAL growth, recovery time and fragmentation.
- Include Minecraft-shaped workloads: UUID point lookups, username changes, player-profile batches, chunk reads/writes, region scans and TTL cleanup.
- Keep benchmark inputs, seeds, machine/compiler settings and baseline results reproducible. Performance claims require before/after measurements.

### Acceptance gates

- Every storage format and transaction boundary has at least one crash/recovery test.
- Stress tests run for a documented duration and data volume in CI/nightly jobs.
- No correctness test is ignored to make a benchmark pass.
- Performance regressions have explicit thresholds and are reviewed alongside memory and tail-latency changes.

## Execution order

1. **A1:** finish PDB6 catalog/row/index codecs and bounded row access APIs; add format-level property and corruption tests.
2. **A2:** integrate PDB6 into a new open/migrate path while retaining PDB5 compatibility; do not switch the default format until crash tests pass.
3. **A3:** make SQL DML, catalog and index mutations one PDB6 transaction; add restart and migration tests.
4. **B1:** define coordinator journal/state machine and participant contract; prove it with two test participants before wiring production stores.
5. **B2:** enlist SQL and KV, then Registry, then EF7; add crash tests at every boundary before each participant is enabled.
6. **C:** implement reachability vacuum only after canonical roots and snapshot/reader pin semantics are stable.
7. **D:** implement secure transport and authorization before enabling non-loopback access.
8. **E:** freeze/version the wire protocol, add streaming and parameter binding, then build the JDBC module.
9. **F:** continuously expand fault, concurrency and Minecraft workload suites across all workstreams.

## Branch and release policy

- Work only in the existing dev, canary, unstable and stable branches.
- Do not create branches, rename branches, delete branches or use main.
- Active implementation starts in dev. Promote only reviewed, tested commits to the other existing branches according to their documented maturity.
- Do not call the project production-ready or create a release until all applicable acceptance gates pass.
- Never perform a migration or destructive vacuum against user data without a verified backup and an explicit operator action.
