# PostemberDB ↔ KotlinScriptLoader integration

This directory contains a separate KSL addon. It does not modify or vendor KotlinScriptLoader; upstream remains untouched.

## Architecture

- PostemberDB remains the Rust database engine and owns the data file.
- The addon registers a postemberdb service with KSL's public addon API.
- Scripts use the versioned POSTEMBERDB JSONL/1 loopback protocol.
- Each operation uses a short-lived TCP connection; asynchronous methods run on a bounded worker pool (1–8 workers, queue capped at 256 requests) to apply backpressure.
- The addon refuses non-loopback hosts because the current PostemberDB server has no TLS or authentication.

## Build

Requirements: JDK 21, Gradle, Paper API 1.21.1, and the matching KSL API JAR.

1. Copy the installed KSL plugin JAR to libs/KotlinScriptLoader.jar (this file is intentionally not committed).
2. From this directory run gradle build.
3. Install build/libs/PostemberDB-KSL-0.1.0.jar beside KotlinScriptLoader.jar in the Paper server's plugins directory.
4. Start PostemberDB on the same machine, for example: postemberdb server 127.0.0.1:7432 ./data/postember.pdb.
5. Configure plugins/PostemberDBKSL/config.yml if the loopback address or port differs.

## Script API

The addon adds io.postemberdb.ksl.* as default imports and registers the postemberdb service.

Example script call:

    postemberDb().queryAsync(
        "SELECT balance FROM player_economy WHERE uuid = ${postemberText(player.uniqueId.toString())}"
    ).thenAccept { result ->
        val balance = result.long(0, 0) ?: 0L
        runSync { player.sendMessage("Balance: $balance") }
    }

Use queryAsync / executeAsync from script callbacks. Synchronous query and execute reject Paper's main thread to prevent server stalls. postemberText safely quotes SQL text values; keep SQL identifiers static. The current wire protocol accepts one SQL statement per line and does not yet expose native prepared statements or a transaction scope in the addon. Requests beyond the bounded async queue are rejected rather than accumulated without limit.

## EF7 Minecraft player profiles

The JSONL/1 bridge also exposes a narrow EF7 player-profile API. These operations use the dedicated EF7 sidecar, not SQL tables:

```kotlin
import java.util.UUID

val profile = PostemberPlayerProfile(
    uuid = player.uniqueId,
    username = player.name,
    firstJoinedMs = player.firstPlayed,
    lastSeenMs = System.currentTimeMillis(),
    playtimeTicks = 0L,
    customData = byteArrayOf()
)

postemberDb().putPlayerProfileAsync(7L, stableRowId, profile)
    .thenRun {
        // Schedule any Bukkit/Paper API work back on the server thread.
    }

postemberDb().getPlayerProfileAsync(7L, player.uniqueId)
    .thenAccept { stored ->
        if (stored != null) {
            val savedUsername = stored.profile.username
            val stableRowId = stored.rowId
        }
    }

``

For high-volume updates, send a batch rather than opening one connection per player:

```kotlin
val upserts = listOf(
    PostemberPlayerProfileUpsert(stableRowId, profile)
)
val removals = emptyList<PostemberPlayerRemoval>()

postemberDb().applyPlayerProfileBatchAsync(7L, upserts, removals)
```

The batch command accepts up to 2,500 total profile operations. The 64 KiB protocol line and 24 KiB per-profile custom-data bridge cap can impose a lower practical batch size.

Available operations are `putPlayerProfileAsync`, `getPlayerProfileAsync`, `getPlayerProfileByNameAsync`, `deletePlayerAsync`, and `applyPlayerProfileBatchAsync`. The batch method accepts `PostemberPlayerProfileUpsert` and `PostemberPlayerRemoval` items and applies all profile/index changes through one EF7 commit. Prefer it for groups of player updates rather than issuing one connection per player. Synchronous variants exist for worker threads and reject calls from Paper's main thread. Profile custom data is capped at 24 KiB by this text/hex JSONL bridge; the native Rust EF7 API supports records up to 64 MiB.

The server stores UUID and case-insensitive username indexes together with the typed profile in EF7. Each profile update is one EF7 CoW-tree publication. These commands are rejected while the connection has an open SQL transaction: SQL and EF7 are separate durability domains and do not yet share a cross-domain commit.

The KSL addon now exposes bounded namespace administration and chunk CRUD in addition to player profiles. Keep namespace IDs stable in plugin configuration; a common layout is a server namespace with one child world namespace per world. Namespace deletion is deliberately non-cascading and succeeds only when the namespace has no children, player records, generic records, or chunks.

## EF7 namespace and chunk API

Create a hierarchy with stable numeric IDs:

```kotlin
val serverNamespace = postemberDb().createNamespaceAsync(
    id = 1L, parentId = null, kind = "server", name = "survival"
).join()

val worldNamespace = postemberDb().createNamespaceAsync(
    id = 10L, parentId = serverNamespace.id, kind = "world", name = "overworld"
).join()

val children = postemberDb().listNamespacesAsync(serverNamespace.id, 32).join()
val found = postemberDb().findNamespaceAsync(serverNamespace.id, "OVERWORLD").join()
```

Namespace kinds are `server`, `world`, `plugin`, `playerdata`, and `custom`. Names are case-insensitive for lookup and must use 1–128 ASCII letters, digits, dots, underscores, or hyphens. Listing through the KSL bridge is capped at 64 entries per request.

Store a bounded chunk payload using signed Minecraft chunk coordinates:

```kotlin
val bytes = byteArrayOf(0x01, 0x02, 0x03)
postemberDb().putChunkAsync(worldNamespace.id, -12, 34, bytes).join()

val chunk = postemberDb().getChunkAsync(worldNamespace.id, -12, 34).join()
if (chunk != null) {
    val storedBytes = chunk.payload
}

postemberDb().deleteChunkAsync(worldNamespace.id, -12, 34).join()
```

The line-oriented JSONL/1 bridge caps chunk payloads at 24 KiB because binary data is hex-encoded inside a 64 KiB request/response line. Reads inspect the stored descriptor length before reading payload pages and reject larger chunks rather than allocating and returning an oversized response. The native Rust API still supports chunk records up to 64 MiB. Large world-chunk transport, chunk-range streaming, and atomic chunk batches are intentionally not exposed through this text bridge; they require a separate bounded binary/framed protocol. All synchronous namespace/chunk methods reject calls on Paper's main thread; use the `Async` variants from gameplay callbacks.

## Protocol and safety limits

- JSONL protocol version: 1, including EF7 player-profile, namespace, and bounded chunk command extensions.
- Maximum request line: 64 KiB; EF7 custom profile and chunk payloads are limited to 24 KiB in this bridge.
- Namespace listing through KSL is capped at 64 metadata entries per request; chunk reads enforce the byte budget before loading payload pages.
- Server client limit and I/O timeouts remain enforced by PostemberDB.
- Loopback only; do not expose port 7432 to a network.
- This is a first integration milestone, not a JDBC driver and not a replacement for KSL's existing SQLite/MySQL/MariaDB/PostgreSQL support.
