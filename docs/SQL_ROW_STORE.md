# Page-backed SQL row store

PostemberDB now has a typed row-record primitive, `SqlRowStore`, built on the native `RecordStore` and EmberTree page files. It is intended to become the physical row layer used by SQL tables.

## API

```rust
use postemberdb::{SqlRowStore, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One store represents one table. The table catalog supplies the column count.
    let mut rows = SqlRowStore::open("data/players.rows", 3)?;
    rows.put(42, &[
        Value::Text("player-uuid".into()),
        Value::Integer(250),
        Value::Boolean(true),
    ])?;

    let player = rows.get(42)?;
    assert_eq!(player.unwrap()[1], Value::Integer(250));

    // Bounded, stable-ID pagination; use the last returned ID as the next cursor.
    let page = rows.scan_rows(None, 100)?;
    for (row_id, values) in page {
        println!("{row_id}: {values:?}");
    }
    Ok(())
}
```

## On-disk format

Each stable row ID is stored as a RecordStore record. The payload uses the versioned `SRW1` format:

- four-byte magic and a little-endian column count;
- one value tag per column;
- signed 64-bit integers, UTF-8 text with a 32-bit byte length, booleans, or NULL;
- strict bounds, UTF-8 validation, boolean validation, schema-width validation, and rejection of trailing bytes.

Large text values can span multiple EPG1 pages. RecordStore validates page and whole-record checksums and publishes descriptor updates through its redo WAL and copy-on-write B+ Tree root.

## Memory and scan bounds

- Default page cache: 128 decoded pages; callers can configure it.
- ID scans do not load row payloads.
- Row scans are capped at 5,000 rows and 256 MiB of encoded payload per call.
- Rows are returned in ascending stable row-ID order.
- Mixed row upserts and deletes can be published atomically in one descriptor-tree root update through `apply_batch`.
- One record is capped at 64 MiB by RecordStore.

## Integration status — important

`SqlRowStore` is currently a standalone storage component. The existing `Database` SQL executor has **not yet** been migrated to use it:

- SQL catalog and table schemas are still persisted in the PDB5 snapshot.
- SQL row data is still serialized into the snapshot and reconstructed into `Table.rows` during `Database::open`.
- SQL transactions still commit through the SQL statement WAL; they are not yet atomically enlisted with SqlRowStore.
- SQL SELECT, UPDATE, and DELETE still operate on the in-memory table representation.
- Row-page reclamation/vacuum and streaming payload reads remain future work.

Do not interpret the presence of this API as proof that SQL tables are already disk-backed. The next integration stage must move table catalog metadata and row-ID allocation into durable storage, route DML through a single recoverable transaction protocol, and make SELECT use bounded page reads. Existing PDB1–PDB5 databases must remain readable and receive an explicit, crash-safe migration path.


`apply_batch` gives upserts precedence over deletes for the same row ID and avoids a redundant delete/reinsert path.
