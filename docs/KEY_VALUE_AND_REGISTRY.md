# Native Key-Value Store and Registry

PostemberDB provides a byte-oriented key/value API and a typed hierarchical registry on top of its own page-backed copy-on-write B+ Tree. These are independent storage domains; neither is represented as a SQL table.

## KeyValueStore

`KeyValueStore` is intended for cache records, plugin state, compact metadata, Minecraft player data, and application-defined binary keys.

```rust
use postemberdb::KeyValueStore;

let mut kv = KeyValueStore::open("data/world.kv")?;
kv.put(b"players/uuid-123", b"serialized-player")?;

let player = kv.get(b"players/uuid-123")?;
let entries = kv.scan_prefix(b"players/", Some(100))?;
kv.delete(b"players/uuid-123")?;
# Ok::<(), postemberdb::DbError>(())
```

- Keys are arbitrary bytes, 1..=1024 bytes.
- Values are arbitrary bytes that fit in one data page.
- `put_many` applies a batch with one CoW root publication.
- `apply_batch` combines removals and upserts into one redo-WAL transaction; upserts win when a key is present in both sets.
- `scan_prefix` returns bytewise-sorted results and accepts an optional limit.
- `delete_prefix` removes a prefix range in one CoW publication.
- An exclusive sidecar lock prevents two processes from opening the same KV store concurrently.
- Data uses separate `.pages`, `.pages.wal`, and `.lock` files.

The store is built on PostemberDB's native EmberTree page format. It does not use SQLite, RocksDB, or another embedded database.

## Registry

`Registry` is a Windows-Registry-inspired hierarchy with hives, nested keys, and named typed values. Paths are case-insensitive and accept either slash separator.

```rust
use postemberdb::{Registry, RegistryValue};

let mut registry = Registry::open("data/server.registry")?;
registry.create_hive("SYSTEM")?;
registry.create_key("SYSTEM\\Database\\Engine")?;
registry.set_value(
    "SYSTEM/Database/Engine",
    "CacheSize",
    RegistryValue::Integer(512),
)?;

let value = registry.get_value("system/database/engine", "cachesize")?;
# Ok::<(), postemberdb::DbError>(())
```

### Hierarchy

- **Hive** — top-level root, such as `SYSTEM`, `USERS`, or `APPLICATIONS`.
- **Key** — a named section below a hive.
- **Value** — a named typed payload stored under a key.
- **Path** — a slash- or backslash-separated sequence of key names.

### Value types

- `RegistryValue::String(String)`
- `RegistryValue::Integer(i64)`
- `RegistryValue::Boolean(bool)`
- `RegistryValue::Binary(Vec<u8>)`
- `RegistryValue::Json(String)`

The JSON variant stores UTF-8 JSON text; callers remain responsible for validating its JSON syntax.

### Operations

- `create_hive` creates a top-level hive.
- `create_key` creates a key and missing ancestors atomically.
- `key_exists` checks a key.
- `set_value`, `get_value`, and `delete_value` manage typed values.
- `list_hives`, `list_subkeys`, and `list_values` enumerate hierarchy.
- `delete_key(path, false)` rejects deletion of non-empty keys.
- `delete_key(path, true)` removes the key and its complete subtree in one batch.

The empty value name is supported as the default value of a key.

## Database integration

For a file-backed SQL database, open independent sidecars from the database handle:

```rust
let db = postemberdb::Database::open("data/server.pbd")?;
let mut kv = db.open_key_value_store()?;
let mut registry = db.open_registry()?;
# Ok::<(), postemberdb::DbError>(())
```

The KV and registry sidecars are separate from the SQL snapshot/WAL and from the derived SQL index mirror. Each store has its own lock and recovery WAL. A database opened only in memory cannot create these persistent sidecars.

## Current boundaries

- KV keys and registry values are limited by the current single-page key/value codec; large blobs need a future overflow-page/blob layer.
- This first version provides exclusive single-process writers, not multi-writer MVCC.
- Registry JSON is stored as text, not parsed or schema-validated by the engine.
- SQL transactions do not yet include KV or Registry writes in the same cross-domain atomic commit.
- Encryption-at-rest, per-hive ACL/RBAC, and audit events are future security work.
