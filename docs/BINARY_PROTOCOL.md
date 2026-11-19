# PostemberDB Binary Protocol v1

Binary/1 is an initial framed transport for trusted loopback clients. It is not a secure remote protocol: the server still binds to loopback only and does not implement TLS, authentication, authorization, or encryption.

## Negotiation

The server retains the legacy text greeting. A client sends this ASCII line as its first command:

```text
POSTEMBERDB BINARY/1
```

The server responds with one binary frame: kind `Pong`, request ID `0`, payload `postemberdb-binary/1-ready`. After this acknowledgement, the connection is binary-only.

## Frame layout

Every frame begins with a 24-byte little-endian header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `PDBF` |
| 4 | 1 | protocol version (`1`) |
| 5 | 1 | frame kind |
| 6 | 2 | flags (must be zero in v1) |
| 8 | 8 | request ID |
| 16 | 4 | payload length |
| 20 | 4 | CRC32 of payload |

The current maximum payload is 8 MiB per frame. Length is validated before allocation. CRC32 detects accidental transmission corruption; it is not cryptographic authentication. Unknown kinds, unsupported flags/versions, oversized lengths, truncated frames, and checksum mismatches are rejected. Clean EOF is distinguished from a partial frame.

## Frame kinds

| Kind | Value | Direction | Payload |
|---|---:|---|---|
| Query | 1 | client → server | UTF-8 SQL statement |
| QueryResult | 2 | server → client | UTF-8 JSON result |
| Error | 3 | server → client | UTF-8 error text |
| Ping / Pong | 4 / 5 | both | opaque bytes echoed by server |
| ChunkPut | 16 | client → server | namespace u64 LE, X i32 LE, Z i32 LE, then payload |
| ChunkGet | 17 | client → server | namespace u64 LE, X i32 LE, Z i32 LE |
| ChunkDelete | 18 | client → server | namespace u64 LE, X i32 LE, Z i32 LE |
| ChunkData | 19 | server → client | empty when missing; otherwise namespace/X/Z header followed by raw bytes |

Each response carries the same request ID as its request. SQL query frames execute one statement. Transaction-control statements are rejected in this initial transport version; JSONL/1 remains the transaction-capable development path.

## Current limits and deliberate gaps

- Single-frame chunk transfers are limited by the 8 MiB frame cap.
- Native EF7 records/chunks remain limited to 64 MiB.
- Multi-frame upload/download streams, chunk-range scans, chunk batches, flow-control windows, request cancellation, and multiplexed concurrent requests are not implemented yet.
- The server processes requests sequentially per connection.
- The protocol is loopback-only and must not be exposed to untrusted networks.

The next transport milestone is a bounded multi-frame stream protocol with declared total length, per-stream IDs, aggregate in-flight byte budgets, explicit commit/abort, and backpressure. The frame codec is separated from the server dispatcher so that this can be added without changing the 24-byte frame header.
