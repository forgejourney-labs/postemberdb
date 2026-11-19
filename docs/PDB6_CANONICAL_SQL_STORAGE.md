# PDB6: canonical page-backed SQL storage

## Status

Design contract for the next storage generation. This is not a claim that PDB6 is implemented. The current PDB5 snapshot plus SQL statement WAL remains authoritative. The PDB6 shared tree now has a bounded overflow-row payload primitive, but Database SQL execution has not yet been migrated to it.

## Implemented PDB6 row-payload primitive

The shared PDB6 store now supports versioned P6R1 row pages and P6D1 row descriptors
under the existing table/row keyspace. A row may span multiple checksummed 4 KiB
extent pages up to 64 MiB. Batches are bounded to 5,000 logical mutations and
256 MiB of aggregate row/logical data.

Row payload pages are allocated and synced before their descriptors become
reachable. The row descriptors can then be published in the same copy-on-write
root update as catalog and index key mutations. A crash before publication can
leave unreachable pages, but cannot expose only part of the logical mutation.
Reads validate page identity, sequence, lengths, and whole-row checksum. Row IDs
can be scanned in bounded keyset pages.

This is **not yet the live SQL row store**. Database still loads PDB5 rows into
memory and commits through the PDB5 SQL WAL. PDB6 row payload pages are not yet
automatically enlisted in Database transactions, and replaced/deleted extents
remain allocated until vacuum is implemented.

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

- [x] Binary catalog/schema codec and corruption tests.
- [x] Shared keyspace and root-manifest codec.
- [x] Bounded overflow row payload pages and descriptors with checksum validation and shared-root batch publication.
- [ ] Transaction redo format and crash-point fault injection.
- [ ] Atomic multi-table root publication and recovery.
- [ ] PDB5 read-only compatibility and restartable migration.
- [ ] Database DML routed through page-backed rows; SQL snapshot no longer stores full row vectors.
- [ ] Lazy point reads, bounded scans, and shared buffer pool.
- [ ] Vacuum/reclamation with active-reader safety.
- [ ] Recovery, migration, and large Minecraft workload benchmarks.

Until these gates pass, PostemberDB must not describe its SQL tables as disk-native or production-ready.

## Explicit PDB5-to-PDB6 copy operation

`Database::migrate_to_pdb6()` performs an explicit copy of the currently
opened PDB5 state into PDB6 catalog, typed row-payload, and index keyspaces.
Table IDs and index IDs are assigned deterministically from sorted catalog
names; stable row IDs are preserved. Row and index writes are bounded to 5,000
mutations per batch. The PDB6 completion manifest is published last.

The operation does not modify the PDB5 snapshot or WAL and does not switch
normal SQL execution to PDB6. If interrupted, PDB5 remains authoritative. An
incomplete PDB6 sidecar must currently be removed by the operator before
restarting the copy; automatic progress tracking and orphan-page reclamation
are not yet implemented. Do not treat the copied sidecar as production-ready
or switch authority until the verification and recovery gates pass.
