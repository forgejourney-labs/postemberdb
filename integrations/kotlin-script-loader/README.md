# PostemberDB ↔ KotlinScriptLoader integration

This directory contains a separate KSL addon. It does not modify or vendor KotlinScriptLoader; upstream remains untouched.

## Architecture

- PostemberDB remains the Rust database engine and owns the data file.
- The addon registers a postemberdb service with KSL's public addon API.
- Scripts use the versioned POSTEMBERDB JSONL/1 loopback protocol.
- Each operation uses a short-lived TCP connection; asynchronous methods are available.
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

Use queryAsync / executeAsync from script callbacks. Synchronous query and execute reject Paper's main thread to prevent server stalls. postemberText safely quotes SQL text values; keep SQL identifiers static. The current wire protocol accepts one SQL statement per line and does not yet expose native prepared statements or a transaction scope in the addon.

## Protocol and safety limits

- JSONL protocol version: 1.
- Maximum SQL statement: 64 KiB.
- Server client limit and I/O timeouts remain enforced by PostemberDB.
- Loopback only; do not expose port 7432 to a network.
- This is a first integration milestone, not a JDBC driver and not a replacement for KSL's existing SQLite/MySQL/MariaDB/PostgreSQL support.
