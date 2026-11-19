# RecordStore: variable-length page-backed records

PostemberDB now includes an independent RecordStore primitive for values that do not fit in one B+ Tree leaf entry. It stores each record across one or more EPG1 pages and indexes record IDs through the native copy-on-write EmberTree B+ Tree.

## Open a store

For a database opened from disk, the RecordStore sidecar can be opened from the Database handle:

```rust
use postemberdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("data/server.pbd")?;
    let mut records = db.open_record_store()?;

    let player_record_id = 42u64; // Use a stable, non-zero application ID.
    records.put(player_record_id, b"serialized player state")?;
    let state = records.get(player_record_id)?;
    assert_eq!(state.as_deref(), Some(b"serialized player state".as_slice()));
    Ok(())
}
```

The sidecar base is <code>server.pbd.rows</code>; it creates the files `.rows.pages`, `.rows.pages.wal`, and `.rows.lock`. The default cache capacity is 128 pages. Configure the capacity through `open_record_store_with_cache_capacity`, or use `RecordStore::open_with_cache_capacity` directly.

## Public API

- `put(id, bytes)` inserts or atomically replaces a record.
- `get(id)` returns the complete record or `None` when it does not exist.
- `contains(id)` checks the record descriptor.
- `delete(id)` removes the descriptor and returns whether it existed.
- `page_cache_stats()` reports cache hits, misses, evictions, residency, and capacity.
- `set_page_cache_capacity(pages)` adjusts the cache at runtime.

IDs are caller-supplied non-zero `u64` values. They are not auto-assigned. An application may use stable SQL row IDs as record IDs, but RecordStore does not automatically observe SQL INSERT/UPDATE/DELETE statements.

## Page and commit layout

Each record has an RDX1 descriptor in the EmberTree B+ Tree. It contains the first data page ID, page count, total length, and a whole-record checksum. RPG1 page payloads contain the record ID, chunk ordinal/count, total length, the same checksum, and the chunk length. The surrounding EPG1 format validates page identity, generation, checksum, and zero padding.

A write first reserves a contiguous page extent and writes/syncs all data pages. Only then does it publish the new descriptor through the page-tree redo WAL and atomic root update. If a crash occurs before publication, the prior value remains the visible one; newly reserved pages may be leaked. The new value is never made visible before its complete data pages are durable.

## Current limits and safety boundaries

- Maximum record size is 64 MiB.
- Reads materialize the complete record into a Vec; streaming I/O is future work.
- Page writes are synced individually; batched page-write scheduling is not implemented yet.
- Replaced and deleted record extents are not reclaimed yet. They remain allocated until reachability-aware vacuum is available.
- The RecordStore sidecar has an exclusive process/handle lock, but it does not join the SQL transaction WAL. SQL + RecordStore atomic transactions are not supported.
- It is currently a standalone sidecar, not the canonical SQL table row store. SQL snapshots still load all rows into memory.
- The whole-record checksum is an accidental-corruption check, not a cryptographic MAC or authentication mechanism.

Do not treat the current RecordStore as a production-ready SQL row backend until vacuum/reclamation, streaming reads, write batching, transaction integration, and crash/fault-injection tests are implemented.
