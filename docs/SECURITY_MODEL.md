# Security model and release gates

## Current posture

PostemberDB is experimental and is not production-ready. The development TCP protocol has no authentication, TLS, authorization model, or stable framing. The current binary therefore restricts binding to explicit loopback IP addresses. Do not expose it to a LAN or the public internet.

The server also enforces a maximum of 128 active client threads, a 64 KiB SQL-line limit, 30-second socket read/write timeouts, UTF-8 validation, and bounded per-connection request parsing. These controls are not a substitute for authentication or a production protocol.

## Threat model

The production design must account for malformed SQL and protocol frames; oversized lengths, integer overflow, allocation and decompression bombs; clients exhausting threads, memory, disk, WAL space, or query time; corrupt/truncated/reordered storage records; concurrent writers and stale snapshots; backup tampering; and secrets leaking into logs or crash reports.

## Required controls

### Network and identity
- TLS with certificate validation for remote connections.
- Authentication through pluggable credential providers; never log plaintext passwords.
- Role-based authorization for databases, schemas, tables, and operations.
- Per-user connection, query, memory, and disk quotas.
- Query deadlines, cancellation, bounded result streaming, and backpressure.
- Protocol version negotiation and strict frame-size limits.

### Storage integrity
- Validate every length, offset, page ID, generation, and count before allocating or seeking.
- Checksums detect accidental corruption; authenticated encryption is separate.
- WAL records are framed, sequenced, checksummed, and synced before commit acknowledgement.
- Recovery is idempotent and tested with fault injection at every write/sync/rename boundary.
- Metadata-root publication and torn-page protection are atomic and recoverable.
- Backup manifests and restored pages are verified before the database is made available.

### Cryptography and keys
- Encryption at rest is planned as an opt-in feature using a vetted AEAD construction and OS/key-management integration.
- Encryption keys are never stored beside the encrypted database.
- Key rotation uses wrapped data-encryption keys and a recoverable migration procedure.
- No custom cryptographic algorithms.

### Resource safety
- Explicit budgets for page cache, indexes, query operators, WAL staging, and connections.
- Maximum row, value, page, frame, batch, and decompressed sizes.
- No unbounded per-client threads or unbounded queues.
- Manual compaction by default; operators inspect fragmentation and schedule bounded work.
- Resource exhaustion returns typed errors without corrupting committed state.

## Security test suite

Release gates include SQL-parser and protocol fuzzing, property tests for page/WAL codecs, corrupted-file tests, decompression-limit tests, concurrency stress, permission tests, and backup/restore drills. A green unit-test run alone does not establish production security.


## Implemented transport authentication and RBAC

- The legacy `server` command remains loopback-only and unauthenticated for local development.
- `server-tls` loads a PEM certificate/private key, enables TLS, and requires authentication before SQL or EF7 operations. Non-loopback binds are rejected unless this TLS mode is active.
- Provision accounts with `postemberdb user add <data-file> <username> <admin|read-only|read-write>`; passwords are prompted without terminal echo and stored as Argon2id PHC hashes in the `.auth` sidecar.
- SQL read/write and EF7 read/write permissions are enforced for authenticated connections. Unknown SQL/EF7 command classes are denied by default.
- TLS uses rustls and PEM certificate/private-key parsing. Clients must validate the server certificate; mutual TLS and certificate-to-user mapping are not yet implemented.
- Authentication audit events are durable, checksummed, sequence-addressable, and exclude password/hash material. `AuthStore::as_admin` requires an authenticated Admin principal and records that actor for administrative mutations; direct legacy local API calls remain labeled `local-api`. Audit retention/export, tamper-evidence beyond per-record checksums, per-user connection/query quotas, authentication rate limiting, password rotation policy enforcement, and database/schema/table-scoped grants remain future work.


## JDBC driver

The standalone `jdbc-driver/` Maven module provides a JDBC 4.2 driver over the versioned JSONL/1 server transport. Use `jdbc:postemberdb://host:7432` with `ssl=true` (default) for TLS, and supply `user`/`password` properties for RBAC. For loopback-only development servers, set `ssl=false` and omit credentials. The driver implements Connection, Statement, PreparedStatement, ResultSet and metadata basics. PreparedStatement currently performs client-side, quote-aware parameter binding; server-side prepared statement frames, streaming ResultSets, generated keys, savepoints, batch execution, and full metadata discovery are not yet supported.
