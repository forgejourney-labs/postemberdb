# EF7 Record TTL

EmberForge 7 supports an optional expiry index for generic records. Expiry timestamps are absolute Unix milliseconds supplied by the caller; EF7 does not read the wall clock implicitly.

## Rust API

- `put_record(namespace_id, record_id, payload)` stores a record without expiry and removes any previous TTL association.
- `put_record_with_expiry(namespace_id, record_id, expires_at_ms, payload)` stores/replaces the record and its expiry index in one CoW publication.
- `get_record_limited(namespace_id, record_id, max_bytes)` checks the descriptor length before loading overflow pages.
- `delete_record(namespace_id, record_id)` removes the descriptor and optional TTL keys atomically.
- `sweep_expired_records(now_ms, limit)` removes at most 10,000 records with expiry <= now_ms in one bounded CoW publication.

TTL index keys are ordered by big-endian expiry, namespace ID, and record ID. A per-record metadata key allows replacement and deletion to remove the previous expiry entry. Payload extents are reclaimed only after the descriptor/index publication succeeds.

The sweeper validates that each TTL index entry agrees with its per-record metadata before mutation. Corrupt or inconsistent TTL metadata fails closed rather than deleting a potentially unrelated record.

## KSL JSONL/1

The addon exposes asynchronous and worker-thread methods:

- `putRecordAsync`
- `putRecordWithExpiryAsync`
- `getRecordAsync`
- `deleteRecordAsync`
- `sweepExpiredRecordsAsync`

The JSONL/1 hex transport caps record payloads at 24 KiB. Native Rust EF7 supports records up to 64 MiB. Use small TTL records through the text bridge; large payloads need the future binary streaming transport.

TTL cleanup is explicit and budgeted. Plugins should schedule sweeps at a suitable cadence and use a modest per-run limit rather than attempting an unbounded cleanup on the server's main thread.
