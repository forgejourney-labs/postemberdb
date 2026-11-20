# PostemberDB Binary Protocol v1

Binary/1 is the framed client/server protocol used by the TLS-enabled PostemberDB server. Remote clients must establish TLS, validate the server certificate against a trusted CA bundle, and verify the expected server name before authenticating. The bundled Rust client has no insecure certificate-validation mode.

The plain server command remains a loopback-only development listener. For remote connections, use server-tls with a PEM certificate and private key.

## Connection sequence

1. The client opens TCP and completes TLS certificate-chain and server-name verification.
2. The server sends its text greeting (PostemberDB version).
3. The client sends AUTH with username and password inside the TLS session.
4. The server verifies the Argon2id password hash and returns an authentication result.
5. The authenticated client sends POSTEMBERDB BINARY/1.
6. The server acknowledges with a Pong frame, request ID 0, and payload postemberdb-binary/1-ready.
7. The connection is binary-only until closed.

The server currently uses server-authenticated TLS without mutual TLS. User identity is established by password authentication inside the TLS channel.

## Frame layout

Every frame begins with a 24-byte little-endian header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic PDBF |
| 4 | 1 | protocol version (1) |
| 5 | 1 | frame kind |
| 6 | 2 | flags (must be zero in v1) |
| 8 | 8 | request ID |
| 16 | 4 | payload length |
| 20 | 4 | CRC32 of payload |

The maximum payload is 8 MiB per frame. Length is validated before allocation. CRC32 detects accidental transmission corruption; it is not cryptographic authentication. TLS provides channel encryption and peer authentication. Unknown kinds, unsupported flags/versions, oversized lengths, truncated frames, and checksum mismatches are rejected. Clean EOF is distinguished from a partial frame.

## Frame kinds

| Kind | Value | Direction | Payload |
|---|---:|---|---|
| Query | 1 | client → server | UTF-8 SQL statement |
| QueryResult | 2 | server → client | UTF-8 JSON result |
| Error | 3 | server → client | UTF-8 error text |
| Ping / Pong | 4 / 5 | both | opaque bytes echoed by server |
| Catalog / CatalogResult | 6 / 7 | client → server / server → client | Empty request; serialized SQL catalog snapshot |
| TablePage / TablePageResult | 8 / 9 | client → server / server → client | JSON table, offset, limit; bounded table-page result |
| ServerStatus / ServerStatusResult | 10 / 11 | client → server / server → client | Empty request; status JSON (Admin permission) |
| AdminRequest / AdminResponse | 12 / 13 | client → server / server → client | Bounded JSON administrative action and result |
| BatchQuery / BatchQueryResult | 14 / 15 | client → server / server → client | JSON statement array; transactional SQL write batch |
| ChunkPut | 16 | client → server | namespace u64 LE, X i32 LE, Z i32 LE, then payload |
| ChunkGet | 17 | client → server | namespace u64 LE, X i32 LE, Z i32 LE |
| ChunkDelete | 18 | client → server | namespace u64 LE, X i32 LE, Z i32 LE |
| ChunkData | 19 | server → client | Empty when missing; otherwise namespace/X/Z header followed by raw bytes |
| ChunkScan / ChunkScanResult | 20 / 21 | client → server / server → client | Reserved for bounded chunk-range scans |

Every response carries the same request ID as its request. Responses are validated against the expected frame kind by the Rust client.

## Authorization

On a TLS listener, every request is checked against the authenticated principal:

- Query, Catalog, and TablePage require the appropriate SQL read/write permission.
- SQL command classes not covered by the RBAC policy are denied.
- BatchQuery requires SqlWrite, accepts 1–500 non-empty SQL write statements, and commits them through one Database transaction. A batch containing a read, unsupported command, or transaction-control statement is rejected.
- ServerStatus requires Admin.
- AdminRequest requires Admin and executes user, role, and audit actions inside AuthStore::as_admin, so durable audit entries identify the authenticated actor.

Supported remote administrative actions include users.list, users.create, users.permissions, users.enabled, users.delete, users.password, roles.list, roles.create, roles.permissions, roles.assign, roles.revoke, roles.delete, and audit.list.

Administrative responses never contain password hashes. Password-reset commands do not return the new password. RBAC changes that would leave no enabled administrator are rejected.

## Rust client

The library exposes RemoteClient::connect(address, server_name, ca_bundle_path, username, password). The CA bundle must be PEM and trusted by the client; the TLS server name must match the certificate. The client has bounded connect/read/write timeouts and validates request IDs and response frame kinds.

Desktop Browser uses this client for remote catalog browsing, SQL, paging, and transactional edits. Web Admin uses it behind its own loopback-only HTTP interface.

## Limits and gaps

- Single-frame chunk transfers are limited by the 8 MiB frame cap.
- Native EF7 records/chunks remain limited to 64 MiB; multi-frame streaming is not implemented.
- Remote table pages are capped at 1,000 rows.
- SQL transaction batches are capped at 500 statements.
- Administrative request frames are capped at 64 KiB; remote audit requests are capped at 500 events.
- The server processes requests sequentially per connection. Multiplexing, request cancellation, query deadlines, per-user quotas, flow-control windows, and backpressure are not implemented.
- Mutual TLS, certificate-to-user mapping, authentication rate limiting, and password rotation policy remain future work.
- Binary/1 transaction batches cover SQL only; they do not combine SQL, KV, Registry, and EF7 into one transaction.
- Legacy JSONL/1 remains a development/compatibility protocol and must not be exposed to untrusted networks.
