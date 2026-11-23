# PDB6: canonical page-backed SQL storage

## Current authority and compatibility

PDB6 is the canonical SQL persistence format after either:

- creating a new database with `Database::create_pdb6()` / `create-pdb6`; or
- explicitly migrating a legacy PDB5 database with `Database::migrate_to_pdb6()` / `migrate-pdb6`.

When `Database::open()` finds a valid, published PDB6 manifest, it opens PDB6 as the SQL authority. PDB6-backed handles do not append to or rewrite the retained PDB5 snapshot/WAL. A database without a published PDB6 manifest remains on its existing PDB5 compatibility path. Opening a PDB5 database does not silently migrate it.

This document describes the current implementation on `dev`. It distinguishes completed behavior from remaining engineering gates; a completed checkbox is not a production-readiness claim.

## What PDB6 stores

PDB6 uses a shared page-backed copy-on-write B+ Tree and one published root/manifest for the SQL catalog, row descriptors, index entries, and table counters. The main pieces are:

- `PCT1` — versioned catalog/schema records.
- `PDM1` — the published database manifest and root metadata.
- `P6D1` — row descriptors that reference row payload pages.
- `P6R1` — checksummed overflow row payload pages.
- `EAM3` — mirrored allocator metadata and durable root-page publication.
- `EPG1` — fixed-size extent data pages.
- A dedicated page-tree redo WAL for copy-on-write page publication.

The shared binary keyspace is versioned and prefix-separated:

| Prefix | Key shape | Purpose |
|---|---|---|
| `0x10` | table ID | Table/catalog metadata |
| `0x20` | table ID + stable row ID | Row descriptor |
| `0x30` | table ID + index ID + encoded key | Secondary-index entries |
| `0x40` | table ID | Monotonic next-row-ID and table counters |
| `0x7f` | fixed catalog key | Database catalog/root metadata |

Integer key components use big-endian encoding so bytewise ordering matches numeric ordering. Key codecs reject unknown tags, invalid lengths, duplicate IDs, and non-canonical encodings.

Rows can span multiple checksummed 4 KiB extent pages up to 64 MiB per row. PDB6 write batches are bounded by mutation count and aggregate payload limits; the row-store primitive supports up to 5,000 logical mutations and 256 MiB aggregate row/logical data per batch. The page-native SQL DML path applies a stricter retained-row budget (up to 5,000 changed rows and 128 MiB of retained row values).

## Open and read path

Opening PDB6 reads and validates the published manifest, catalog/schema metadata, index definitions, and row counts. It does not eagerly hydrate every row payload into RAM.

- `SELECT` and table-page reads fetch validated row payloads from disk in bounded row-ID batches.
- `LIMIT` can stop a scan early when no requested ordering requires consuming the remainder.
- `COUNT(*)` counts through a streaming scan without materializing a result row for every stored row.
- Supported equality and bounded range lookups can use persistent index entries; unsupported key encodings or unavailable derived/index paths use the safe in-memory fallback where applicable.
- `ORDER BY` is supported, but a disk-backed ordered result may need to retain and sort its matching rows; this is not a streaming external sort.

## Write path and its limits

PDB6 committed writes publish catalog changes, row descriptors/payload references, index mutations, counters, and the new manifest through a shared CoW root. New row payload pages are written and validated before descriptors become reachable. A successful root publication is the atomic visibility boundary.

The following page-native paths are implemented:

- `insert_many` writes bounded row batches without hydrating the entire target table.
- Plain single-statement `INSERT`, single-column `UPDATE`, and `DELETE` through `Database::execute()` scan row IDs in bounded batches and retain only changed rows. The current page-native DML path has a 5,000-row ceiling and a 128 MiB retained-row-value budget.
- Index mutations for affected rows are published with row/catalog changes through one CoW root.

Important boundaries remain:

- UPDATE and DELETE still scan rows to evaluate their predicates unless a supported lookup narrows the work; full predicate pushdown and a cost-aware planner are unfinished.
- Multi-statement SQL transactions, `DatabaseSession`, and UPSERT still use compatibility hydration paths to preserve current constraint and transaction semantics. These operations can materialize the participating table state.
- The shared bounded SQL row cache, page pinning, and database-wide memory-budget enforcement are unfinished.
- The canonical SQL row store is the PDB6 catalog/descriptor/payload keyspace. The separate `RecordStore` and `SqlRowStore` APIs remain useful storage primitives, but they are not a separate SQL executor backend.

## Indexes and stable row identity

SQL index entries use stable table-local row IDs rather than mutable vector positions. INSERT adds affected row IDs; UPDATE changes index entries only when indexed values change; DELETE removes the affected keys; UPSERT rekeys the affected row. The row-ID-to-position map remains an in-memory lookup aid and can be rebuilt after vector compaction.

The persistent B+ Tree performs copy-on-write path updates and bounded range traversal. Recovery validates reachable-tree structure, separators, ordered child ranges, cycles, multiply referenced pages, page images, and redo records. Persistent sibling pointers and byte-balanced redistribution of underfull siblings are not currently required for correctness and remain potential performance/space improvements.

## Durability, migration, and recovery

The PDB6 root is published only after the write path has validated staged mutations and synchronized the required redo/page data. The allocator keeps mirrored EAM3 metadata slots; one damaged copy can be repaired from the valid copy, while two invalid copies fail closed. The page tree replays pending redo during open before exposing the store.

PDB5-to-PDB6 migration is explicit and non-destructive:

1. Copy table definitions, stable row IDs, row payloads, and index entries in bounded batches.
2. Read back and verify catalog records, row payloads, index keys, checksums, and catalog cardinality.
3. Publish the PDB6 completion manifest last.
4. Retain the original PDB5 snapshot/WAL for compatibility and recovery.

An interrupted migration can leave incomplete PDB6 sidecars. Automatic progress resume and cleanup are not yet complete; an operator may need to remove the incomplete sidecars before retrying. Never remove or overwrite the original PDB5 source as part of an unverified migration.

## Named snapshots and vacuum

PDB6 exposes:

- `create_pdb6_snapshot(name)` — durably retain the current immutable root.
- `list_pdb6_snapshots()` — list retained snapshot names.
- `delete_pdb6_snapshot(name)` — remove a named root reference; pages are reclaimed later.
- `vacuum_pdb6()` — trace the current root and retained snapshot roots before reclaiming eligible pages.

The `.pdb6.snapshots` registry uses versioned `PSN1` records, bounded names/counts, checksums, and atomic temporary-file replacement with file and parent-directory sync.

Vacuum can reclaim unreachable tree pages across the current and named roots. Overflow reclamation is conservative: while any named snapshot exists, all `P6R1` overflow pages are retained because old snapshot descriptors are not yet independently traced. Once no named snapshots remain, a later vacuum can trace current descriptors and reclaim orphan overflow pages. Unknown page types are retained.

PDB6 currently serializes page-store access with an exclusive file lock; concurrent shared readers and active-reader epochs/pins are not implemented. Crash-resumable vacuum progress and fault injection across every allocator-release boundary remain open.

## Remaining implementation gates

- Shared bounded SQL buffer pool, page pinning, and explicit global memory budgets.
- Bounded staging for multi-statement transactions, `DatabaseSession`, and UPSERT without compatibility table hydration.
- Full SQL predicate pushdown, composite secondary indexes, cost-aware planning, `EXPLAIN`, and streaming/external sort.
- Persistent B+ Tree sibling links or redistribution only if benchmarks demonstrate a need.
- Active-reader epochs/pins and per-snapshot overflow descriptor tracing.
- Crash-resumable vacuum and broader subprocess/device-level fault injection.
- Verified full/incremental backup and restore.
- Optional page compression with strict decompression limits.
- Explicit in-place update mode only after an independent page-recovery mechanism is designed and tested.

PDB6 is a real page-backed SQL persistence path, but PostemberDB remains experimental and must not be described as production-ready until the recovery, concurrency, backup/restore, resource-budget, security, compatibility, and repeatable performance release gates are complete.
