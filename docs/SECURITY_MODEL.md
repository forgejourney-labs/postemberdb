# Security model and release gates

## Current posture

PostemberDB remains experimental and is not production-ready. It has two distinct listener modes:

- The plain server command is an unauthenticated development listener restricted to explicit loopback addresses.
- The server-tls command loads a PEM certificate/private key, enables TLS, and requires password authentication before SQL, EF7, or Binary/1 requests. Non-loopback binds are permitted only for this TLS mode.

Remote TLS clients must validate the server certificate chain against a trusted CA and verify the expected server name. The bundled Rust RemoteClient has no insecure certificate-validation switch. The server currently uses server-authenticated TLS without mutual TLS; user identity is established through Argon2id password authentication inside the TLS channel.

The web PostemberDB Admin tool is a separate operator application. It binds to loopback only. Its local Rust backend can establish a remote TLS session; Admin is not a remotely hosted web service and must not be exposed through a reverse proxy or public interface.

The server enforces a maximum of 128 active client threads, 64 KiB SQL line limits, 30-second socket timeouts, UTF-8 validation, bounded frame sizes, and bounded per-connection request parsing. These controls are not a substitute for a production security review.

## Threat model

The production design must account for malformed SQL and protocol frames; oversized lengths, integer overflow, allocation and decompression bombs; clients exhausting threads, memory, disk, WAL space, or query time; corrupt/truncated/reordered storage records; concurrent writers and stale snapshots; backup tampering; and secrets leaking into logs or crash reports.

## Implemented network and identity controls

- TLS server mode with rustls and PEM certificate/private-key parsing.
- Client-side certificate-chain validation against an explicit PEM CA bundle and hostname validation.
- Argon2id password hashes stored in the .auth sidecar; password hashes are never returned by user-list or administration endpoints.
- Authenticated Binary/1 requests with request-ID and frame-kind validation.
- RBAC enforcement for SQL reads/writes, EF7 reads/writes, catalog browsing, table pages, transaction batches, server status, and administrative operations.
- SQL write batches are bounded and committed through one SQL transaction.
- Remote user, role, and audit operations require an authenticated Admin principal and execute inside AuthStore::as_admin for actor-attributed audit records.
- Password reset events are audited without recording passwords or hashes.
- Administrative changes that would leave no enabled administrator are rejected.
- The plain server listener remains loopback-only and unauthenticated for local development.
- The web Admin listener remains loopback-only; its remote backend uses the verified TLS client.

## Remaining network and identity gaps

- Mutual TLS and certificate-to-user mapping are not implemented.
- Authentication rate limiting, account lockout policy, password rotation policy enforcement, per-user connection/query/memory/disk quotas, query deadlines, and request cancellation are not implemented.
- Database/schema/table-scoped grants and finer-grained audit permissions are not implemented.
- Admin's local HTTP interface is designed for loopback use only. It has no remote web login or CSRF token flow; do not publish or reverse-proxy it.
- Audit retention/export policy and tamper evidence beyond per-record checksums remain future work.
- Binary/1 requests are sequential per connection; multiplexing, streaming results, backpressure, and bounded server-side query cancellation remain future work.

## Storage integrity

- Validate every length, offset, page ID, generation, and count before allocating or seeking.
- Checksums detect accidental corruption; authenticated encryption is separate.
- WAL records are framed, sequenced, checksummed, and synced before commit acknowledgement.
- Recovery is idempotent and tested with fault injection at write/sync/rename boundaries.
- Metadata-root publication and torn-page protection must remain atomic and recoverable.
- Backup manifests and restored pages must be verified before the database is made available.

## Cryptography and keys

- Encryption at rest is planned as an opt-in feature using a vetted AEAD construction and OS/key-management integration.
- Encryption keys are never stored beside the encrypted database.
- Key rotation uses wrapped data-encryption keys and a recoverable migration procedure.
- No custom cryptographic algorithms.

## Resource safety

- Explicit budgets for page cache, indexes, query operators, WAL staging, and connections.
- Maximum row, value, page, frame, batch, and decompressed sizes.
- No unbounded per-client threads or unbounded queues.
- Manual compaction by default; operators inspect fragmentation and schedule bounded work.
- Resource exhaustion returns typed errors without corrupting committed state.

## Security test suite and release gates

Release gates include SQL-parser and protocol fuzzing, property tests for page/WAL codecs, corrupted-file tests, decompression-limit tests, TLS certificate/name-mismatch tests, RBAC permission matrix tests, concurrency stress, and backup/restore drills. A green unit-test run alone does not establish production security.

## JDBC driver

The standalone jdbc-driver Maven module provides a JDBC 4.2 driver over the versioned JSONL/1 server transport. Use jdbc:postemberdb://host:7432 with ssl=true (default) and user/password properties for TLS/RBAC. The desktop Browser and web Admin use the verified TLS Binary/1 client; the JDBC driver remains a separate protocol implementation.
