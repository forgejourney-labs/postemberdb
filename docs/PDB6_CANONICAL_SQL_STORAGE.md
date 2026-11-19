# PDB6: canonical page-backed SQL storage

## Status

Design contract for the next storage generation. This is not a claim that PDB6 is implemented. The current PDB5 snapshot plus SQL statement WAL remains authoritative, and SqlRowStore remains an independent page-backed prototype.

## Non-negotiable invariants

1. **One SQL transaction, one durable commit decision.** Catalog changes, row inserts/updates/deletes, index-definition changes, and row-ID allocator changes in a transaction must be covered by one commit record. Independent per-table root publications are not sufficient.
2. **WAL before data publication.** The commit record is appended and synced before a transaction becomes visible. Recovery replays committed transactions idempotently and ignores/truncates only a torn final frame; corruption in a complete frame fails closed.
3. **One atomic database root.** A committed transaction publishes a single database root/manifest that names the catalog and all table/index roots. Readers must never observe a catalog from one transaction and rows from another.
4. **Large values are overflow records.** B+ tree leaves contain bounded descriptors, not arbitrary row payloads. Row payload pages are checksummed and validated before descriptor publication.
5. **No eager table reconstruction on open.** Opening a PDB6 database loads bounded metadata and roots. SELECT resolves only needed pages; full scans stream bounded batches.
6. **PDB5 compatibility.** Existing PDB1-PDB5 files remain readable. Migration is explicit, restartable, and never overwrites the last known-good source before the new root is durable.

## Logical keyspace

The initial shared tree uses versioned, binary, prefix-separated keys:

- 0x10 | table_id:u64be — table/catalog metadata.
- 0x20 | table_id:u64be | row_id:u64be — row descriptor.
- 0x30 | table_id:u64be | index_id:u64be | encoded_key — secondary-index posting/row references.
- 0x40 | table_id:u64be — monotonic next-row-ID and table counters.
- 0x7f — database catalog/root metadata.

All integer key components use big-endian encoding so bytewise order matches numeric order. Key codecs are versioned and reject unknown tags, invalid lengths, duplicate IDs, and non-canonical encodings.

## Transaction protocol

1. Parse and validate the complete statement/transaction against a staged catalog view.
2. Assign stable table and row IDs without publishing allocator changes.
3. Encode row overflow pages and index mutations into a bounded transaction batch.
4. Append a checksummed redo frame containing transaction ID, base root generation, new catalog/root metadata, page images or redo operations, and a commit marker; sync it.
5. Write/sync immutable data pages and new CoW tree pages.
6. Atomically publish the new root manifest with generation compare-and-swap semantics; sync metadata.
7. Acknowledge commit only after durable root publication. If a crash occurs after the commit record but before root publication, recovery completes publication. If the commit record is absent, the transaction is not committed.

The exact ordering of page images and redo payload must be finalized against the existing EAM3 metadata and page-tree recovery implementation before coding. The existing independent RecordStore WAL cannot serve as the transaction coordinator for multi-table SQL.

## Read path and resource bounds

- Point lookup: catalog/schema lookup, index/root traversal, then only the selected row overflow pages.
- Equality/range scans: ordered B+ tree cursor with LIMIT and bounded page batches.
- Buffer pool: bounded shared cache with pin counts, dirty-page rules, and explicit memory budget; no per-table unbounded cache multiplication.
- Every batch has independent row-count, encoded-byte, page-count, and WAL-frame limits.
- Vacuum/reclamation is generation-aware: pages are reusable only after no published root or active reader can reference them.

## Migration

PDB5 migration must use a side-by-side target file or a new versioned sidecar, persist a migration state marker, copy catalog and rows in bounded batches, validate row counts/checksums/index definitions, sync the target, then atomically publish the PDB6 manifest. Reopening during migration resumes or safely discards the incomplete target; the original PDB5 file remains intact until validation succeeds.

## Implementation gates

- [ ] Binary catalog/schema codec and corruption tests.
- [ ] Shared keyspace and root-manifest codec.
- [ ] Transaction redo format and crash-point fault injection.
- [ ] Atomic multi-table root publication and recovery.
- [ ] PDB5 read-only compatibility and restartable migration.
- [ ] Database DML routed through page-backed rows; SQL snapshot no longer stores full row vectors.
- [ ] Lazy point reads, bounded scans, and shared buffer pool.
- [ ] Vacuum/reclamation with active-reader safety.
- [ ] Recovery, migration, and large Minecraft workload benchmarks.

Until these gates pass, PostemberDB must not describe its SQL tables as disk-native or production-ready.