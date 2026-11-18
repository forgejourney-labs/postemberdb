# KSL adapter status at the end of `dev`

This document records the KSL integration boundary at the point where feature development moves to `canary`. It describes what is present in the repository, not what has been validated in a live Minecraft server.

| Capability | Status |
|---|---|
| Separate PostemberDB KSL addon | Implemented in this repository |
| Service registration through the public KSL API | Implemented in source; runtime compatibility still needs an end-to-end test |
| Asynchronous queries and writes | Implemented in source through the bounded worker pool |
| JSONL/1 protocol | Implemented in Rust and covered by Rust protocol tests |
| JDBC driver | Not implemented |
| Network `PreparedStatement` / parameter-array support | Not implemented |
| Transaction scope through the KSL client | Not implemented |
| Request cancellation when a script is reloaded | Not implemented |
| Full Paper + KSL end-to-end test suite | Pending |

## Integration boundary

- The addon is separate from KSL core and does not modify the upstream KotlinScriptLoader repository.
- It registers a `postemberdb` service through KSL's public addon API. It is not a JDBC driver and does not make PostemberDB selectable through KSL's existing `database.type` setting.
- The addon sends one SQL statement per short-lived loopback connection using JSONL/1. The Rust server retains its legacy line protocol.
- Async calls run on a bounded worker pool. Synchronous calls must not run on Paper's main tick thread.
- The bridge rejects non-loopback hosts. The current server has no TLS, authentication, or RBAC and must not be exposed to untrusted networks.
- The Rust CI job does not compile the Kotlin addon. Building it requires JDK 21, Gradle, Paper API, and a locally supplied KSL plugin API JAR; that JAR is intentionally not committed.
- A successful Rust CI run therefore does not establish that the addon has compiled or passed a live Paper/KSL test.

## Next KSL milestones on `canary`

1. Add parameter arrays and server-side statement binding to JSONL/1.
2. Add connection-scoped transactions and explicit rollback behavior to the protocol and client.
3. Add bounded result sizes, query deadlines, and cancellation semantics that survive script reload.
4. Implement and test a JDBC driver only after the protocol semantics are stable.
5. Compile against the actual KSL API JAR and run automated tests against a real Paper + KSL test server.

Do not mark any pending item complete based solely on source-level scaffolding. Update this table only after the corresponding build or integration test succeeds.
