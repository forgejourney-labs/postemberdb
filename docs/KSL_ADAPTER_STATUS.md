# KSL adapter implementation status

This page distinguishes source-level implementation from features that have been validated against an installed KotlinScriptLoader and Paper server.

| Capability | Status |
|---|---|
| Separate PostemberDB KSL addon and public service registration | Implemented in source; live KSL validation pending |
| Asynchronous SQL query and write API | Implemented in source |
| Typed SQL parameters over JSONL/1 | Implemented in Rust and exposed by the Kotlin client |
| Atomic parameterized SQL batch | Implemented; one request commits or rolls back as one database transaction |
| KSL transaction builder | Implemented as transactionAsync / transaction over SQL_BATCH |
| Bounded persistent JSONL connection pool | Implemented in source; maximum 1..=8 connections per client |
| EF7 player, namespace, chunk, record and TTL operations | Implemented within documented wire limits |
| Native server-side prepared-statement handles and parsed-plan cache | Not implemented |
| JDBC driver | A separate basic jdbc-driver module exists; full JDBC compliance and live validation remain pending |
| KSL selectable JDBC dialect / database.type integration | Not implemented |
| Request cancellation after script reload | Not implemented |
| Full Paper + KSL end-to-end test suite | Pending |

## SQL access from KSL

Dynamic values should use typed parameters, not string interpolation:

\`\`\`kotlin
postemberDb().queryParamsAsync(
    "SELECT balance FROM player_economy WHERE uuid = ?",
    player.uniqueId.toString()
).thenAccept { result ->
    val balance = result.long(0, 0) ?: 0L
    runSync { player.sendMessage("Balance: $balance") }
}
\`\`\`

The JSONL/1 SQL_PARAMS command accepts up to 1,024 positional parameters per statement. Supported wire values are NULL, signed 64-bit integer, text, and boolean. The request remains bounded by the server's 64 KiB JSONL line limit. SQL identifiers cannot be supplied as value parameters.

Use SQL_BATCH for a group of changes that must commit together:

\`\`\`kotlin
postemberDb().transactionAsync {
    execute("UPDATE player_economy SET balance = balance + ? WHERE uuid = ?", 100L, uuid)
    execute("INSERT INTO player_audit (uuid, action) VALUES (?, ?)", uuid, "reward")
}.thenAccept { results ->
    // Each result corresponds to one statement in the committed batch.
}
\`\`\`

A batch accepts 1..=500 statements and returns the result for each statement. If one statement fails, the whole database transaction is rolled back. SQL_BATCH cannot be nested inside the connection-scoped BEGIN/COMMIT/ROLLBACK session. It does not enlist EF7, KV, or Registry mutations; cross-domain atomicity is a separate feature.

The Kotlin transaction builder only collects SQL statements while the block executes. Network I/O occurs asynchronously on the bounded worker pool after the block has returned.

## Connection and threading model

- JSONL/1 connections are reused in a bounded pool. The maximum pool size equals the configured KSL worker count (1..=8).
- Idle sockets are retired after 25 seconds, before the Rust server's 30-second client I/O timeout.
- The asynchronous worker queue remains capped at 256 tasks. Saturated queues and connection pools reject work rather than growing without bound.
- Synchronous SQL methods reject calls from Paper's primary thread. Gameplay callbacks should use the Async variants.
- The addon independently requires loopback destinations. Do not expose the plain JSONL/1 listener to an untrusted network.
- The addon currently uses JSONL/1, not the Binary/1 transport. It does not provide multi-frame streaming for large SQL result sets or EF7 payloads.

## Remaining work

1. Add native prepared-statement handles and safe parsed-plan reuse after the statement AST and invalidation rules are stable.
2. Add result-size budgets, request deadlines, cancellation semantics that survive script reload, and streaming result pages.
3. Validate the addon against the locally installed KSL API JAR and Paper dependencies; run integration tests on a real Paper + KSL test server.
4. Compare KSL workloads with the current SQLite backend using identical data, concurrency, and hardware before making performance claims.

The Rust CI job does not compile the Kotlin addon. Source-level changes and Rust protocol tests do not establish that the addon compiles against a particular local KSL API JAR or works on a running Minecraft server.
