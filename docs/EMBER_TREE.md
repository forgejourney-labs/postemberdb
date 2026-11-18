# EmberTree Storage Model

**EmberTree** is the official name for PostemberDB's native B+ tree and the evolving tree-oriented storage engine. The repository owns both an in-memory B+ tree in `src/storage/btree.rs` and an experimental page-backed CoW B+ tree in `src/storage/page_tree.rs`; neither wraps an external database or tree library. The in-memory tree supports insertion/replacement, point lookup, deletion and rebalancing, inclusive range scans, node splitting, strict occupancy validation, and stack-based successor-leaf traversal. SQL equality indexes use this in-memory implementation. The page-backed implementation stores immutable leaf/internal pages in an `ExtentFile`, journals page images, and atomically publishes its root through mirrored metadata. This is a real implementation of our own B+ tree, but it is not yet the SQL index source of truth or the complete storage engine.

## Goals and invariants

1. A committed transaction survives process failure according to its configured durability mode.
2. Every page and metadata structure is versioned and checksummed. Lengths, offsets, generations, and page references are validated before allocation or decoding.
3. Hot-key updates do not rewrite whole tables or snapshots.
4. Memory and disk budgets are explicit and observable.
5. Automatic garbage collection and defragmentation are disabled by default. Reclamation is operator-controlled.
6. Embedded Rust, the JVM adapter, and the server share the same storage semantics.

## Logical layout

EmberTree separates logical identity from physical placement:

- Catalog tree: schemas, columns, constraints, index definitions, and table roots.
- Primary B+ tree: primary keys (including UUID and composite keys) to stable row IDs.
- Secondary B+ trees: indexed values to row IDs, with row generations.
- Record pages: packed row versions and overflow pages for large values.
- Extent map: logical page IDs to physical extents; tracks free, reserved, pinned, and reclaimable space.
- Commit metadata: monotonically increasing commit sequence, transaction outcome, and snapshot roots.
- WAL: framed recovery records sufficient to recover committed changes and ignore incomplete transactions.

A bounded buffer pool caches pages by logical page ID and generation. Cache misses read only requested pages. Range scans walk adjacent B+ tree leaves in bounded batches. Rows are not stored in one ever-growing in-memory Vec.

## Current page-file prototype

The existing PPG1 format wraps the current serialized database snapshot in fixed 4 KiB pages. It still loads the complete snapshot and is not a lazy page store.

A separate experimental `ExtentFile` primitive now provides:
- EPG1 fixed-size data pages with logical page IDs, generations, payload lengths, checksums bound to page identity/generation, and canonical zero padding.
- EAM3 allocator metadata mirrored into two alternating slots, with monotonically increasing generations, checksums, and a durable optional B+ tree root; EAM2 remains readable and migrates on the next metadata commit.
- First-fit contiguous extent allocation, adjacent free-range coalescing, page-ID reuse, and persisted allocator statistics.
- Reopen tests for pages and metadata, plus corruption tests.

The `page_tree` module (the persistent part of EmberTree) builds immutable B+ tree leaf/internal pages in an `ExtentFile`, records page images in a separate redo WAL, syncs those pages, and atomically publishes the new root through EAM3 metadata. `Database::open` now checks the sidecars `<database>.pages` and `<database>.pages.btree.wal` when present, then replays pending tree redo before returning the database. This recovery is deliberately separate from the SQL transaction WAL: the persistent tree is not yet connected to SQL index lookups, the catalog, row pages, or SQL transaction commits. It supports both full `replace_all` rebuilds and incremental copy-on-write `insert_or_replace` / `insert_many` operations. Incremental batches copy only affected search paths, split overflowing leaf/internal pages, write one redo record, and publish one new root; unchanged subtrees retain their page IDs. The page-tree API also exposes `remove_many`: it applies ordered key deletions with one redo record and root publication, ignores missing keys, merges adjacent same-level siblings when the combined encoded page fits, and collapses unary roots. Non-root unary internal pages can remain when a merge is not possible; byte-balanced redistribution and minimum page occupancy are not yet enforced. The SQL mirror still uses full rebuilds, and old pages remain allocated until a reachability-aware reclamation protocol exists. The allocator is single-owner and still has no page pinning or transactional reclamation. Each successful metadata commit syncs the new generation to both slots; one damaged copy is repaired from the other, while two invalid copies fail closed. A write error poisons the current handle until reopen. Do not treat this isolated primitive as the active SQL storage backend yet.

## Update modes

StorageMode::Cow and StorageMode::InPlace are planned explicit choices.

### CoW mode

Changed tree and data pages are written to new extents. A commit publishes new roots only after the WAL is durable. Old extents remain pinned while snapshots or readers reference them. This enables cheap snapshots and strong isolation, but may increase write amplification and fragmentation.

### In-place mode

Eligible pages are updated in place only after the WAL contains enough recovery information. Double-write or equivalent torn-page protection is mandatory. In-place mode reduces extent churn but has different snapshot and reader-pinning costs. It must never weaken WAL ordering or page integrity.

Both modes share catalog, index semantics, checksums, and recovery tests. The selected mode is persisted in the database format and cannot be silently changed at open.

## Protection against frequent updates

For Minecraft balances, playtime, cooldowns, and region metadata, the write path should:

1. Collect updates into bounded transaction batches.
2. Coalesce repeated updates to the same row/key before page flush.
3. Append one transaction frame and sync according to durability configuration.
4. Update only affected index paths and record pages.
5. Apply backpressure instead of allowing unbounded queues.
6. Checkpoint by explicit byte/record/time budget.

Expose write amplification, WAL growth, cache hit rate, queue depth, and p50/p95/p99 latency. Performance claims require repeatable benchmarks on identical hardware and datasets.

## Manual disk and memory budgets

A future StorageConfig will independently budget page-cache bytes, index-cache bytes, query/sort memory, WAL staging, open files, concurrent readers, maximum database size, reserved free space, and defragmentation bytes per step. The allocator must report configured, used, pinned, reclaimable, and free bytes and fail predictably when a budget is exhausted.

## Fragmentation API contract

The following API is planned and must not be exposed as implemented until the page allocator and extent map exist.

FragmentationStats will report managed bytes, live bytes, total reusable free-extent bytes, fragmented-hole bytes, hole percentage, live record count, total record extents, average extents per record, and largest free extent.

DefragProgress will report bytes moved, estimated bytes remaining, and completion state.

Planned methods:
- db.fragmentation_stats()
- db.defrag_key(key)
- db.defrag_range(start, end)
- db.defrag_step(max_bytes)

Definitions:
- managed_bytes excludes file headers and unallocated tail capacity.
- free_extent_bytes counts all reusable extents.
- fragmented_hole_bytes counts reusable extents trapped between live extents; free tail capacity is reported separately.
- hole_percent = fragmented_hole_bytes / managed_bytes * 100; zero when managed_bytes is zero.
- average_extents_per_record = total_record_extents / live_records; zero when no records exist.
- Defrag moves only whole pages/extents that fit the caller's budget. It never splits a record or moves a pinned page.
- Relocation is journaled and durable before the new pointer becomes visible. After a crash, either old or new mapping remains valid.
- defrag_step(0) is a no-op. No background compaction runs unless a future explicit opt-in policy is configured.

Range/key defrag is incremental and returns progress; it must never trigger a hidden full-file rewrite.

## Compression

Compression is per page or bounded block, never one giant database stream. Planned codecs are none, LZ4, and Snappy. Each page stores a codec ID and compressed/uncompressed lengths. Decompression checks the declared output size against configured limits before allocating. Checksums cover stored bytes and decoded-content metadata. Benchmarks, not assumptions, determine defaults.

## Snapshots, backups, and commit history

A snapshot records a consistent commit sequence and root set. CoW snapshots pin referenced extents; in-place mode requires a journaled snapshot strategy. Online backup captures a consistent snapshot plus the WAL range needed to recover it. Incremental backups record changed page generations since a base backup. Restore verifies manifests, checksums, format versions, and WAL continuity before publishing the restored database.

Each successful transaction receives a monotonic commit sequence. Optional human-readable labels point to a snapshot/commit. Commit metadata is immutable; labels and retention policies are separate catalog operations.

## Compatibility and migration

The current PPG1/PDB4 snapshot format is a prototype, not EmberTree. The new format needs a new magic/version, feature flags, endian rules, page-size declaration, and migration tooling. Existing database files must remain readable or have a tested export/import path. No in-place format upgrade without a verified backup.

## Implementation order

1. Complete the in-memory B+ tree with deletion/rebalancing, efficient stack-based successor-leaf navigation, and property tests (SQL equality index integration is now in place).
2. Specify and property-test the EmberTree on-disk format.
3. Implement page-file abstraction, durable extent allocator, dual metadata roots, and checksummed page I/O.
4. Implement bounded buffer pool and persist B+ tree nodes as pages.
5. Add fault-injection tests and secure the development server.
6. Implement CoW first; add in-place only after recovery proofs and fault-injection coverage.
7. Add manual fragmentation metrics and budgeted relocation.
8. Add compression, snapshots, verified backup/restore, and CLI commands.
9. Integrate the Rust/JVM boundary and run KSL/Minecraft load tests.

## Incremental page-tree write API

`insert_or_replace` updates one key and `insert_many` applies ordered mutations with last-write-wins behavior for duplicate keys. Each batch stages immutable page images, journals them together, syncs them, then atomically publishes the root. A crash before the redo record is durable can leak reserved but unreachable page IDs; it cannot expose a partially updated root. A crash after the redo record is durable is completed by recovery. Deletion, SQL delta integration, and page reclamation are not implemented yet.

## Non-goals

No claims of Btrfs-level maturity, no background compaction by default, no unbounded result materialization, no unbounded connection threads, and no unbounded decompression.

## TTL (Time-To-Live)

TTL is planned as indexed expiry metadata, not a timer thread per row. A table may define an expiry column or a TTL policy. The engine maintains an expiry-ordered tree so expired rows can be found without scanning every record.

The intended semantics are:
- expiration timestamps use a documented UTC epoch unit and checked integer conversion;
- reads can optionally treat expired rows as invisible at a caller-supplied or transaction-stable timestamp;
- physical deletion is a logged transaction, not an unjournaled memory cleanup;
- expiry work is explicitly budgeted with a maximum number of rows/pages per call;
- automatic expiry workers are disabled by default, preserving operator control and predictable latency;
- snapshots and backups preserve the expiry metadata and commit sequence.

TTL must not silently delete data based on an untrusted or backwards-moving wall clock. Clock source, transaction timestamp, retention grace period, and restore behavior are part of the public contract.


## Incremental deletion

`remove_many` performs copy-on-write deletion for an ordered batch. Missing keys are ignored. Changed search paths are staged as immutable pages, adjacent same-level siblings are coalesced when their combined contents fit in one page, and unary roots are collapsed before a single redo record and root publication. When siblings cannot be coalesced, a non-root unary internal node may remain. Redistribution of underfull siblings, a minimum-occupancy invariant, and page reclamation are still pending. SQL DELETE transactions now update the derived persistent index mirror through this API after the canonical SQL WAL commit.


## SQL transaction delta integration

After the canonical SQL WAL commit is durable, the database now compares the previous and next in-memory index entry sets and applies only removed row-reference keys with `remove_many` and new keys with `insert_many`. Unchanged persistent subtrees are retained by copy-on-write path updates. If the mirror update fails, the SQL transaction remains committed and the engine falls back to in-memory indexes; startup can rebuild the derived mirror from the SQL snapshot/WAL. This first delta implementation still scans all in-memory index entries to calculate the set difference, so delta discovery is O(total indexed entries). Because SQL row references are positional, deleting a row can shift references for later rows and produce a larger delta. Stable row identifiers and direct mutation journaling are future optimizations. SQL rows/catalog are still not page-backed.


## Atomic mixed mutation batches

The persistent page-tree API now exposes `apply_mutations`, which accepts removals and upserts together. It evaluates removals first and upserts second (so an upsert wins for a key present in both lists), stages the combined copy-on-write changes, writes one redo record, and publishes one root. SQL transaction delta synchronization now uses this combined operation instead of publishing a delete root followed by a separate insert root. This closes the intermediate-root window between the two halves of one derived-index update. The SQL WAL remains authoritative; mirror failures still fall back to in-memory indexes. The mixed-batch API does not change the current O(N) delta discovery or positional row-reference limitations.


## Stable SQL row identifiers

SQL rows now carry monotonically allocated table-local `u64` identifiers. Deleting an earlier row changes its in-memory vector position but never changes the identifiers of surviving rows. The derived EmberTree mirror stores these identifiers instead of positional offsets; query execution resolves returned IDs through a table-local ID-to-position map and validates every reference. This removes the large index-delta amplification previously caused by row shifts after DELETE.

The snapshot payload version is now PDB5 and stores each row identifier plus the next-ID counter, including when a table is empty after deletions. PDB4 and earlier snapshots remain readable: legacy rows receive deterministic identifiers in their existing row order, and the next checkpoint writes PDB5. Zero is reserved for synthetic aggregate result rows and is rejected in persisted table rows and persistent index entries. IDs are monotonically allocated and are not reused after deletion or checkpoint. The SQL WAL remains authoritative, and the derived EmberTree mirror is rebuilt from canonical snapshot/WAL state during open.

Delta discovery still compares complete in-memory index-entry sets (O(total indexed entries)); direct mutation journaling remains future work. Stable IDs solve positional churn, not the full-scan cost.


## Direct SQL index mutation journal

SQL transactions now collect persistent index-key changes at the point where rows are inserted, updated, deleted, or upserted. Creating an index over an existing table records its initial entries once. Each journal item contains the fully namespaced EmberTree key (including the stable row ID) and whether it is inserted or removed.

After the canonical SQL WAL commit is synced, the derived EmberTree receives the journal through one mixed removal/upsert CoW batch, one redo record, and one root publication. The transaction path no longer scans every in-memory index entry to calculate a before/after set difference. A full scan remains on database open/rebuild, and unsupported key encodings deliberately disable the persistent mirror so SQL queries safely use in-memory indexes.
