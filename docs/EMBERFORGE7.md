# EmberForge 7 (EF7)

EmberForge 7 is PostemberDB's dedicated persistent record engine for Minecraft servers and KotlinScriptLoader (KSL). It is a separate logical storage engine built on the PDB6 physical extent-page allocator and copy-on-write B+ tree. EF7 owns independent sidecar files and does not overwrite PDB6 or the earlier experimental EFE sidecars.

## Engine layout

For a database path such as `world.pbd`, EF7 uses:

- `world.pbd.ef7.pages` — EPG1 physical pages and mirrored EAM3 allocator metadata.
- `world.pbd.ef7.wal` — CoW B+ tree redo log for EF7 logical keys.
- `world.pbd.ef7.lock` — exclusive process lock.

Logical entries share one persistent B+ tree root. Large record payloads live in checksummed contiguous extent-page runs; tree values contain compact descriptors. Payload pages are written before the descriptor tree redo/root publication. Replaced/deleted extents are released only after publication.

### Key families

| Key prefix | Purpose |
|---|---|
| `0x50` | Namespace metadata |
| `0x51` | Generic record descriptors |
| `0x52` | Player UUID index |
| `0x53` | Player-name index |
| `0x54` | Player row identity metadata |
| `0x55` | Minecraft chunk descriptors |
| `0x56` | Parent-scoped namespace-name index |
| `0x57` | Generic-record TTL index ordered by expiry, namespace, and record ID |
| `0x58` | Per-record TTL metadata used to replace/delete expiry entries |

Keys use big-endian numeric components for stable ordering. Signed chunk coordinates flip the sign bit before encoding, so B+ tree order follows signed X then signed Z order.

## Namespace engine

Namespaces form a parent/child hierarchy and can represent servers, worlds, plugins, player-data areas, or custom domains.

Public API:

- `create_namespace(id, parent_id, kind, name)`
- `get_namespace(id)`
- `find_namespace(parent_id, name)`
- `list_namespaces(parent_id, limit)`
- `delete_namespace(id)`

Names are canonical lowercase ASCII identifiers of 1–128 characters using letters, digits, dot, underscore, or hyphen. Names are unique beneath each parent. A parent must already exist. Namespace deletion is deliberately non-cascading: it is refused while the namespace has child namespaces, player indexes, generic records, player identity metadata, or chunk descriptors.

## Player data engine

EF7 supports raw player records and a typed Minecraft profile model.

Typed profile fields:

- 16-byte UUID.
- Username (1–16 ASCII letters, digits, or underscore).
- First-join and last-seen timestamps in Unix milliseconds.
- Playtime in ticks.
- Arbitrary plugin-owned custom bytes.

Public API:

- `put_player(namespace_id, row_id, uuid, payload)`
- `get_player(namespace_id, uuid)`
- `delete_player(namespace_id, row_id, uuid)`
- `put_player_profile(namespace_id, row_id, profile)`
- `get_player_profile(namespace_id, uuid)`
- `get_player_profile_by_name(namespace_id, username)`
- `scan_player_profiles(namespace_id, after_uuid, limit)`
- `search_player_profiles_by_name(namespace_id, prefix, limit)`
- `apply_player_batch(upserts, removals)` for raw records
- `apply_player_profile_batch(upserts, removals)` for typed profile records

UUID and name indexes are namespace-scoped. Player row identity metadata remembers the current UUID and optional canonical username. Renaming a player or changing the UUID removes obsolete index entries and writes the new record, identity, and indexes through one B+ tree publication. The typed profile batch accepts up to 2,500 mixed profile upserts/removals and 256 MiB of aggregate encoded profile payload, preserving name indexes across the whole batch commit. Duplicate UUIDs and usernames are rejected within a namespace. Reads cross-check the index, identity metadata, and typed profile instead of trusting a stale index entry.

The UUID scan uses an exclusive cursor and a bounded result limit. Name-prefix scans are case-insensitive and bounded. Both cap a single result page at 10,000 entries.

## Generic records and TTL

Generic plugin records support an optional absolute Unix-millisecond expiry:

- `put_record(namespace_id, record_id, payload)` stores a non-expiring record and clears any prior TTL association.
- `put_record_with_expiry(namespace_id, record_id, expires_at_ms, payload)` publishes the descriptor and expiry indexes together.
- `get_record_limited(namespace_id, record_id, max_bytes)` checks the descriptor length before reading overflow pages.
- `delete_record(namespace_id, record_id)` removes the descriptor and its optional TTL keys in one publication.
- `sweep_expired_records(now_ms, limit)` removes up to 10,000 records per call whose expiry is at or before the supplied timestamp.

Expiry keys use big-endian components so the tree orders them by deadline. A metadata key per record identifies its current deadline, allowing a replacement to remove the old index entry. The sweep validates index/metadata agreement, publishes all removals in one CoW redo/root update, then reclaims payload extents. The caller supplies time and cleanup budget; EF7 does not start a background timer.

## Minecraft chunk storage

World chunk data can be stored directly by signed chunk coordinates:

- `put_chunk(world_namespace_id, chunk_x, chunk_z, payload)`
- `get_chunk(world_namespace_id, chunk_x, chunk_z)`
- `delete_chunk(world_namespace_id, chunk_x, chunk_z)`
- `scan_chunks(world_namespace_id, min_x, min_z, max_x, max_z, limit)`
- `apply_chunk_batch(world_namespace_id, upserts, removals)

Chunk descriptors are their own B+ tree entries and point to the same checked overflow-page format. Updating a chunk publishes one new descriptor root and reclaims the previous extent afterward. Region-level batches accept up to 2,500 upserts/removals and 256 MiB of aggregate payload, publishing all chunk-index changes through one redo/root update. Duplicate and overlapping coordinates are rejected before page allocation. Rectangular scans iterate ordered X columns and issue bounded Z-range scans. A query can cover at most 4,096 X columns and return at most 10,000 chunks.

## KSL JSONL/1 adapter

The optional KotlinScriptLoader addon exposes EF7 namespace, profile, chunk, and generic-record operations through the loopback-only JSONL/1 bridge. The Rust server additionally exposes the initial Binary/1 transport, and the Kotlin client now has opt-in framed SQL and single-frame chunk methods:

- Namespace administration: `NS_CREATE`, `NS_GET`, `NS_FIND`, `NS_LIST`, and `NS_DELETE`.
- Typed player profiles: `PROFILE_PUT`, `PROFILE_GET`, `PROFILE_GET_NAME`, `PLAYER_DELETE`, and `PROFILE_BATCH`.
- Chunk CRUD: `CHUNK_PUT`, `CHUNK_GET`, and `CHUNK_DELETE`.

The adapter caps namespace listing at 64 entries per request. Since chunk bytes are hex-encoded inside a 64 KiB line, the KSL bridge accepts at most 24 KiB per profile custom-data payload and 24 KiB per chunk payload. Chunk reads inspect the persistent descriptor's recorded length before loading payload pages; a larger native chunk is rejected before its bytes are materialized for the response. The native Rust API remains capable of 64 MiB records/chunks.

The initial Binary/1 transport and Kotlin client support single-frame chunk transfers up to 8 MiB. Multi-frame chunk streaming, atomic chunk batches, range scans over the wire, flow control, and aggregate in-flight budgets remain pending. JSONL/1 remains capped at 24 KiB for hex-encoded chunk and record payloads. All bridge calls remain loopback-only, and EF7 operations cannot join a SQL transaction.

## Bounds and validation

- Maximum generic/player/chunk payload: 64 MiB per record.
- Maximum extent chain: 16,384 pages.
- Maximum player batch: 2,500 total operations.
- Maximum aggregate player upsert payload per batch: 256 MiB.
- Maximum namespace list page: 10,000.
- Maximum UUID/name scan page: 10,000.
- Maximum chunk scan: 4,096 X columns and 10,000 returned chunks.
- Record descriptors, chunk headers, namespace metadata, player identities, and typed profiles validate lengths, versions, sequence numbers, and checksums.

The current checksum is a non-cryptographic corruption-detection checksum. It is not an authentication code and does not protect against malicious modification.

## Transaction and recovery boundaries

Each player batch, namespace create/delete, single chunk update, and single player identity update uses the EF7 CoW B+ tree redo path and one root publication. Payload pages are durable before the logical descriptor is published. Old payload extents are reclaimed after the root update succeeds.

The EF7 sidecar lock prevents multiple processes from concurrently opening the same EF7 store. Page-tree recovery replays the EF7 redo log and validates the reachable tree on open.

## Compatibility and current limitations

The first EF7 record/chunk format remains version 1. Existing EF7 raw player records without the newer identity metadata can be reverse-resolved by a bounded UUID scan during updates/deletes; this compatibility path is capped at 100,000 UUID index entries per namespace.

EF7 is still a standalone Minecraft/KSL record engine, not the canonical SQL storage backend. It does not yet replace PDB5 SQL snapshots/WAL, coordinate commits with SQL/KV/Registry sidecars, provide general user-defined secondary indexes, or expose a JDBC driver. EF7's redo publication and allocator behavior still need targeted crash-point fault injection, long-running multi-process tests, and realistic Minecraft/KSL end-to-end benchmarks. Do not treat it as production-ready until those gates are complete.
