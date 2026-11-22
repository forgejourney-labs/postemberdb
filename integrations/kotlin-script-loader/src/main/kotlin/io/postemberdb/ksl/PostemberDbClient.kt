package io.postemberdb.ksl

import kotlinx.serialization.json.*
import org.bukkit.Bukkit
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32
import java.util.concurrent.atomic.AtomicLong
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.Closeable
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.Semaphore
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID

data class PostemberSqlStatement(
    val sql: String,
    val parameters: List<Any?> = emptyList()
)

/**
 * Collects parameterized statements for one atomic server-side SQL transaction.
 * The block only builds the batch; database I/O happens on the bounded worker pool.
 */
class PostemberSqlBatch internal constructor() {
    private val pending = ArrayList<PostemberSqlStatement>()

    fun execute(sql: String, vararg parameters: Any?) {
        pending += PostemberSqlStatement(sql, parameters.toList())
    }

    fun query(sql: String, vararg parameters: Any?) {
        pending += PostemberSqlStatement(sql, parameters.toList())
    }

    internal fun snapshot(): List<PostemberSqlStatement> = pending.toList()
}

data class PostemberQueryResult(
    val message: String,
    val affectedRows: Long,
    val columns: List<String>,
    val rows: List<List<Any?>>
) {
    fun string(row: Int, column: Int): String? = rows[row][column] as? String
    fun long(row: Int, column: Int): Long? = (rows[row][column] as? Number)?.toLong()
    fun boolean(row: Int, column: Int): Boolean? = rows[row][column] as? Boolean
}

data class PostemberPlayerProfile(
    val uuid: UUID,
    val username: String,
    val firstJoinedMs: Long,
    val lastSeenMs: Long,
    val playtimeTicks: Long,
    val customData: ByteArray = ByteArray(0)
)

data class PostemberStoredPlayerProfile(
    val rowId: Long,
    val profile: PostemberPlayerProfile
)

data class PostemberPlayerProfileUpsert(
    val rowId: Long,
    val profile: PostemberPlayerProfile
)

data class PostemberPlayerRemoval(
    val rowId: Long,
    val uuid: UUID
)

data class PostemberNamespace(
    val id: Long,
    val parentId: Long?,
    val kind: String,
    val name: String
)

data class PostemberChunk(
    val namespaceId: Long,
    val chunkX: Int,
    val chunkZ: Int,
    val payload: ByteArray
)

data class PostemberStoredRecord(
    val namespaceId: Long,
    val recordId: Long,
    val payload: ByteArray
)

/** Thread-safe loopback bridge to the PostemberDB JSONL/1 protocol. */
private class PooledJsonlConnection(
    val socket: Socket,
    val reader: BufferedReader,
    val writer: BufferedWriter
) {
    @Volatile
    var lastUsedAtNanos: Long = System.nanoTime()

    fun closeQuietly() {
        try { socket.close() } catch (_: Exception) {}
    }
}

class PostemberDbClient internal constructor(
    private val host: String,
    private val port: Int,
    private val connectTimeoutMs: Int,
    private val readTimeoutMs: Int,
    workerThreads: Int
) : Closeable {
    private val closed = AtomicBoolean(false)
    private val nextBinaryRequestId = AtomicLong(1)
    private val workerCount = workerThreads.coerceIn(1, 8)
    private val executor: ExecutorService = ThreadPoolExecutor(
        workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(256), ThreadPoolExecutor.AbortPolicy()
    )
    private val json = Json { ignoreUnknownKeys = true }

    // Keep at most one live JSONL socket per bounded worker. The server accepts
    // multiple line requests on one connection, so KSL avoids a TCP/TLS handshake
    // for every SQL statement without creating an unbounded connection queue.
    private val jsonlPermits = Semaphore(workerCount)
    private val idleJsonlConnections = ArrayBlockingQueue<PooledJsonlConnection>(workerCount)
    private val jsonlConnections = ConcurrentHashMap.newKeySet<PooledJsonlConnection>()

    init {
        require(port in 1..65535) { "PostemberDB port must be between 1 and 65535" }
        require(connectTimeoutMs in 100..30000) { "connect-timeout-ms must be between 100 and 30000" }
        require(readTimeoutMs in 100..120000) { "read-timeout-ms must be between 100 and 120000" }
        require(InetAddress.getByName(host).isLoopbackAddress) {
            "PostemberDB currently accepts loopback connections only; configure 127.0.0.1 or ::1"
        }
    }

    fun query(sql: String): PostemberQueryResult {
        check(!Bukkit.isPrimaryThread()) { "Use queryAsync() on Paper's main thread" }
        return request(sql)
    }

    fun execute(sql: String): Long {
        check(!Bukkit.isPrimaryThread()) { "Use executeAsync() on Paper's main thread" }
        return request(sql).affectedRows
    }

    fun queryAsync(sql: String): CompletableFuture<PostemberQueryResult> =
        CompletableFuture.supplyAsync({ request(sql) }, executor)

    /**
     * Execute SQL with typed, positional parameters. Values are sent separately
     * from the SQL text and encoded as explicit integer/text/boolean/null types.
     */
    fun queryParams(sql: String, vararg parameters: Any?): PostemberQueryResult {
        check(!Bukkit.isPrimaryThread()) { "Use queryParamsAsync() on Paper's main thread" }
        return parameterizedRequest(sql, parameters.toList())
    }

    fun executeParams(sql: String, vararg parameters: Any?): Long {
        check(!Bukkit.isPrimaryThread()) { "Use executeParamsAsync() on Paper's main thread" }
        return parameterizedRequest(sql, parameters.toList()).affectedRows
    }

    fun queryParamsAsync(sql: String, vararg parameters: Any?): CompletableFuture<PostemberQueryResult> =
        CompletableFuture.supplyAsync({ parameterizedRequest(sql, parameters.toList()) }, executor)

    fun executeParamsAsync(sql: String, vararg parameters: Any?): CompletableFuture<Long> =
        CompletableFuture.supplyAsync({ parameterizedRequest(sql, parameters.toList()).affectedRows }, executor)

    /** Submit a bounded statement batch as one atomic database transaction. */
    fun executeBatch(statements: List<PostemberSqlStatement>): List<PostemberQueryResult> {
        check(!Bukkit.isPrimaryThread()) { "Use executeBatchAsync() on Paper's main thread" }
        return batchRequest(statements)
    }

    fun executeBatchAsync(statements: List<PostemberSqlStatement>): CompletableFuture<List<PostemberQueryResult>> =
        CompletableFuture.supplyAsync({ batchRequest(statements) }, executor)

    /**
     * Convenient transaction builder for KSL scripts:
     *
     * postemberDb().transactionAsync {
     *     execute("UPDATE players SET coins = coins + ? WHERE uuid = ?", 25, uuid)
     *     execute("INSERT INTO audit_log (uuid, action) VALUES (?, ?)", uuid, "reward")
     * }
     *
     * The builder performs no I/O while the block runs. All statements are then
     * validated and committed by the server in a single atomic request.
     */
    fun transaction(block: PostemberSqlBatch.() -> Unit): List<PostemberQueryResult> {
        check(!Bukkit.isPrimaryThread()) { "Use transactionAsync() on Paper's main thread" }
        val batch = PostemberSqlBatch().apply(block)
        return batchRequest(batch.snapshot())
    }

    fun transactionAsync(block: PostemberSqlBatch.() -> Unit): CompletableFuture<List<PostemberQueryResult>> {
        val batch = PostemberSqlBatch().apply(block).snapshot()
        return executeBatchAsync(batch)
    }


    fun queryBinary(sql: String): PostemberQueryResult {
        check(!Bukkit.isPrimaryThread()) { "Use queryBinaryAsync() on Paper's main thread" }
        require(sql.isNotBlank()) { "SQL statement cannot be blank" }
        val response = binaryRequest(BINARY_QUERY, sql.toByteArray(StandardCharsets.UTF_8))
        if (response.kind != BINARY_QUERY_RESULT) throw PostemberDbException("unexpected binary query response kind")
        val jsonResponse = json.parseToJsonElement(response.payload.toString(StandardCharsets.UTF_8)).jsonObject
        return jsonResponse.toQueryResult()
    }

    fun queryBinaryAsync(sql: String): CompletableFuture<PostemberQueryResult> =
        CompletableFuture.supplyAsync({ queryBinary(sql) }, executor)

    fun putChunkBinary(namespaceId: Long, chunkX: Int, chunkZ: Int, payload: ByteArray): Unit {
        check(!Bukkit.isPrimaryThread()) { "Use putChunkBinaryAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        require(payload.size <= MAX_BINARY_FRAME_PAYLOAD - 16) {
            "Binary/1 single-frame chunk payload limit is 8 MiB minus 16 bytes"
        }
        val request = ByteBuffer.allocate(16 + payload.size).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(namespaceId).putInt(chunkX).putInt(chunkZ).put(payload).array()
        val response = binaryRequest(BINARY_CHUNK_PUT, request)
        if (response.kind != BINARY_QUERY_RESULT) throw PostemberDbException("unexpected binary chunk-put response")
    }

    fun putChunkBinaryAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int,
        payload: ByteArray
    ): CompletableFuture<Unit> =
        CompletableFuture.supplyAsync({ putChunkBinary(namespaceId, chunkX, chunkZ, payload); Unit }, executor)

    fun getChunkBinary(namespaceId: Long, chunkX: Int, chunkZ: Int): PostemberChunk? {
        check(!Bukkit.isPrimaryThread()) { "Use getChunkBinaryAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        val request = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(namespaceId).putInt(chunkX).putInt(chunkZ).array()
        val response = binaryRequest(BINARY_CHUNK_GET, request)
        if (response.kind != BINARY_CHUNK_DATA) throw PostemberDbException("unexpected binary chunk-get response")
        if (response.payload.isEmpty()) return null
        if (response.payload.size < 16) throw PostemberDbException("truncated binary chunk response")
        val metadata = ByteBuffer.wrap(response.payload, 0, 16).order(ByteOrder.LITTLE_ENDIAN)
        val returnedNamespace = metadata.long
        val returnedX = metadata.int
        val returnedZ = metadata.int
        if (returnedNamespace != namespaceId || returnedX != chunkX || returnedZ != chunkZ) {
            throw PostemberDbException("binary chunk response identity does not match request")
        }
        return PostemberChunk(returnedNamespace, returnedX, returnedZ, response.payload.copyOfRange(16, response.payload.size))
    }

    fun getChunkBinaryAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int
    ): CompletableFuture<PostemberChunk?> =
        CompletableFuture.supplyAsync({ getChunkBinary(namespaceId, chunkX, chunkZ) }, executor)

    fun deleteChunkBinary(namespaceId: Long, chunkX: Int, chunkZ: Int): Boolean {
        check(!Bukkit.isPrimaryThread()) { "Use deleteChunkBinaryAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        val request = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(namespaceId).putInt(chunkX).putInt(chunkZ).array()
        val response = binaryRequest(BINARY_CHUNK_DELETE, request)
        if (response.kind != BINARY_QUERY_RESULT) throw PostemberDbException("unexpected binary chunk-delete response")
        val body = json.parseToJsonElement(response.payload.toString(StandardCharsets.UTF_8)).jsonObject
        return body["affected_rows"]?.jsonPrimitive?.longOrNull == 1L
    }

    fun deleteChunkBinaryAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int
    ): CompletableFuture<Boolean> =
        CompletableFuture.supplyAsync({ deleteChunkBinary(namespaceId, chunkX, chunkZ) }, executor)


    fun putRecord(namespaceId: Long, recordId: Long, payload: ByteArray): Unit {
        check(!Bukkit.isPrimaryThread()) { "Use putRecordAsync() on Paper's main thread" }
        require(namespaceId > 0 && recordId > 0) { "namespaceId and recordId must be positive" }
        require(payload.size <= MAX_EF7_WIRE_RECORD_BYTES) {
            "EF7 JSONL/1 record payload limit is $MAX_EF7_WIRE_RECORD_BYTES bytes"
        }
        val encoded = payload.toLowerHex().ifEmpty { "-" }
        protocolRequestLine("EF7 RECORD_PUT $namespaceId $recordId $encoded")
    }

    fun putRecordAsync(
        namespaceId: Long,
        recordId: Long,
        payload: ByteArray
    ): CompletableFuture<Unit> =
        CompletableFuture.supplyAsync({ putRecord(namespaceId, recordId, payload); Unit }, executor)

    fun putRecordWithExpiry(
        namespaceId: Long,
        recordId: Long,
        expiresAtMs: Long,
        payload: ByteArray
    ): Unit {
        check(!Bukkit.isPrimaryThread()) { "Use putRecordWithExpiryAsync() on Paper's main thread" }
        require(namespaceId > 0 && recordId > 0 && expiresAtMs >= 0) {
            "namespaceId, recordId and expiresAtMs must be non-negative (IDs must be positive)"
        }
        require(payload.size <= MAX_EF7_WIRE_RECORD_BYTES) {
            "EF7 JSONL/1 record payload limit is $MAX_EF7_WIRE_RECORD_BYTES bytes"
        }
        val encoded = payload.toLowerHex().ifEmpty { "-" }
        protocolRequestLine("EF7 RECORD_PUT_TTL $namespaceId $recordId $expiresAtMs $encoded")
    }

    fun putRecordWithExpiryAsync(
        namespaceId: Long,
        recordId: Long,
        expiresAtMs: Long,
        payload: ByteArray
    ): CompletableFuture<Unit> =
        CompletableFuture.supplyAsync(
            { putRecordWithExpiry(namespaceId, recordId, expiresAtMs, payload); Unit }, executor
        )

    fun getRecord(namespaceId: Long, recordId: Long): PostemberStoredRecord? {
        check(!Bukkit.isPrimaryThread()) { "Use getRecordAsync() on Paper's main thread" }
        require(namespaceId > 0 && recordId > 0) { "namespaceId and recordId must be positive" }
        return protocolRequestLine("EF7 RECORD_GET $namespaceId $recordId").readRecord()
    }

    fun getRecordAsync(
        namespaceId: Long,
        recordId: Long
    ): CompletableFuture<PostemberStoredRecord?> =
        CompletableFuture.supplyAsync({ getRecord(namespaceId, recordId) }, executor)

    fun deleteRecord(namespaceId: Long, recordId: Long): Boolean {
        check(!Bukkit.isPrimaryThread()) { "Use deleteRecordAsync() on Paper's main thread" }
        require(namespaceId > 0 && recordId > 0) { "namespaceId and recordId must be positive" }
        return protocolRequestLine("EF7 RECORD_DELETE $namespaceId $recordId")["affected_rows"]
            ?.jsonPrimitive?.longOrNull == 1L
    }

    fun deleteRecordAsync(
        namespaceId: Long,
        recordId: Long
    ): CompletableFuture<Boolean> =
        CompletableFuture.supplyAsync({ deleteRecord(namespaceId, recordId) }, executor)

    fun sweepExpiredRecords(nowMs: Long, limit: Int = 256): Long {
        check(!Bukkit.isPrimaryThread()) { "Use sweepExpiredRecordsAsync() on Paper's main thread" }
        require(nowMs >= 0) { "nowMs must be non-negative" }
        require(limit in 1..10_000) { "TTL sweep limit must be 1..10000" }
        return protocolRequestLine("EF7 RECORD_SWEEP $nowMs $limit")["affected_rows"]
            ?.jsonPrimitive?.longOrNull ?: 0L
    }

    fun sweepExpiredRecordsAsync(
        nowMs: Long,
        limit: Int = 256
    ): CompletableFuture<Long> =
        CompletableFuture.supplyAsync({ sweepExpiredRecords(nowMs, limit) }, executor)

    fun executeAsync(sql: String): CompletableFuture<Long> =
        queryAsync(sql).thenApply { it.affectedRows }


    fun createNamespace(
        id: Long,
        parentId: Long?,
        kind: String,
        name: String
    ): PostemberNamespace {
        check(!Bukkit.isPrimaryThread()) { "Use createNamespaceAsync() on Paper's main thread" }
        require(id > 0) { "namespace ID must be positive" }
        require(parentId == null || parentId > 0) { "parent namespace ID must be positive" }
        val normalizedKind = kind.lowercase()
        require(normalizedKind in setOf("server", "world", "plugin", "playerdata", "custom")) {
            "namespace kind must be server, world, plugin, playerdata or custom"
        }
        require(name.matches(Regex("[A-Za-z0-9._-]{1,128}"))) {
            "namespace name must be 1..128 ASCII letters, digits, '.', '_' or '-'"
        }
        val command = "EF7 NS_CREATE $id " + parentToken(parentId) + " $normalizedKind $name"
        val response = protocolRequestLine(command)
        return response.readNamespace()
            ?: throw PostemberDbException("EF7 create namespace response omitted namespace")
    }

    fun createNamespaceAsync(
        id: Long,
        parentId: Long?,
        kind: String,
        name: String
    ): CompletableFuture<PostemberNamespace> =
        CompletableFuture.supplyAsync({ createNamespace(id, parentId, kind, name) }, executor)

    fun getNamespace(id: Long): PostemberNamespace? {
        check(!Bukkit.isPrimaryThread()) { "Use getNamespaceAsync() on Paper's main thread" }
        require(id > 0) { "namespace ID must be positive" }
        return protocolRequestLine("EF7 NS_GET $id").readNamespace()
    }

    fun getNamespaceAsync(id: Long): CompletableFuture<PostemberNamespace?> =
        CompletableFuture.supplyAsync({ getNamespace(id) }, executor)

    fun findNamespace(parentId: Long?, name: String): PostemberNamespace? {
        check(!Bukkit.isPrimaryThread()) { "Use findNamespaceAsync() on Paper's main thread" }
        require(parentId == null || parentId > 0) { "parent namespace ID must be positive" }
        require(name.matches(Regex("[A-Za-z0-9._-]{1,128}"))) { "invalid namespace name" }
        val command = "EF7 NS_FIND " + parentToken(parentId) + " $name"
        return protocolRequestLine(command).readNamespace()
    }

    fun findNamespaceAsync(parentId: Long?, name: String): CompletableFuture<PostemberNamespace?> =
        CompletableFuture.supplyAsync({ findNamespace(parentId, name) }, executor)

    fun listNamespaces(parentId: Long? = null, limit: Int = 64): List<PostemberNamespace> {
        check(!Bukkit.isPrimaryThread()) { "Use listNamespacesAsync() on Paper's main thread" }
        require(parentId == null || parentId > 0) { "parent namespace ID must be positive" }
        require(limit in 1..64) { "KSL namespace listing is limited to 1..64 entries per request" }
        val command = "EF7 NS_LIST " + parentToken(parentId) + " $limit"
        val response = protocolRequestLine(command)
        return response["namespaces"]?.jsonArray?.map { it.jsonObject.toNamespace() } ?: emptyList()
    }

    fun listNamespacesAsync(
        parentId: Long? = null,
        limit: Int = 64
    ): CompletableFuture<List<PostemberNamespace>> =
        CompletableFuture.supplyAsync({ listNamespaces(parentId, limit) }, executor)

    fun deleteNamespace(id: Long): Boolean {
        check(!Bukkit.isPrimaryThread()) { "Use deleteNamespaceAsync() on Paper's main thread" }
        require(id > 0) { "namespace ID must be positive" }
        return protocolRequestLine("EF7 NS_DELETE $id")["affected_rows"]?.jsonPrimitive?.longOrNull == 1L
    }

    fun deleteNamespaceAsync(id: Long): CompletableFuture<Boolean> =
        CompletableFuture.supplyAsync({ deleteNamespace(id) }, executor)

    fun putChunk(namespaceId: Long, chunkX: Int, chunkZ: Int, payload: ByteArray): Unit {
        check(!Bukkit.isPrimaryThread()) { "Use putChunkAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        require(payload.size <= MAX_EF7_WIRE_CHUNK_BYTES) {
            "EF7 JSONL/1 chunk payload limit is $MAX_EF7_WIRE_CHUNK_BYTES bytes"
        }
        val encoded = payload.toLowerHex().ifEmpty { "-" }
        protocolRequestLine("EF7 CHUNK_PUT $namespaceId $chunkX $chunkZ $encoded")
    }

    fun putChunkAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int,
        payload: ByteArray
    ): CompletableFuture<Unit> =
        CompletableFuture.supplyAsync({ putChunk(namespaceId, chunkX, chunkZ, payload); Unit }, executor)

    fun getChunk(namespaceId: Long, chunkX: Int, chunkZ: Int): PostemberChunk? {
        check(!Bukkit.isPrimaryThread()) { "Use getChunkAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        return protocolRequestLine("EF7 CHUNK_GET $namespaceId $chunkX $chunkZ").readChunk()
    }

    fun getChunkAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int
    ): CompletableFuture<PostemberChunk?> =
        CompletableFuture.supplyAsync({ getChunk(namespaceId, chunkX, chunkZ) }, executor)

    fun deleteChunk(namespaceId: Long, chunkX: Int, chunkZ: Int): Boolean {
        check(!Bukkit.isPrimaryThread()) { "Use deleteChunkAsync() on Paper's main thread" }
        require(namespaceId > 0) { "world namespace ID must be positive" }
        return protocolRequestLine(
            "EF7 CHUNK_DELETE $namespaceId $chunkX $chunkZ"
        )["affected_rows"]?.jsonPrimitive?.longOrNull == 1L
    }

    fun deleteChunkAsync(
        namespaceId: Long,
        chunkX: Int,
        chunkZ: Int
    ): CompletableFuture<Boolean> =
        CompletableFuture.supplyAsync({ deleteChunk(namespaceId, chunkX, chunkZ) }, executor)

    private fun parentToken(parentId: Long?): String = parentId?.toString() ?: "-"


    fun putPlayerProfile(
        namespaceId: Long,
        rowId: Long,
        profile: PostemberPlayerProfile
    ): Unit {
        check(!Bukkit.isPrimaryThread()) { "Use putPlayerProfileAsync() on Paper's main thread" }
        require(namespaceId > 0 && rowId > 0) { "namespaceId and rowId must be positive" }
        require(profile.customData.size <= MAX_EF7_WIRE_CUSTOM_BYTES) {
            "EF7 JSONL/1 customData limit is $MAX_EF7_WIRE_CUSTOM_BYTES bytes"
        }
        val command = listOf(
            "EF7", "PROFILE_PUT", namespaceId.toString(), rowId.toString(),
            profile.uuid.toString().replace("-", ""), profile.username,
            profile.firstJoinedMs.toString(), profile.lastSeenMs.toString(),
            profile.playtimeTicks.toString(),
            profile.customData.toLowerHex().ifEmpty { "-" }
        ).joinToString(" ")
        protocolRequestLine(command)
    }

    fun putPlayerProfileAsync(
        namespaceId: Long,
        rowId: Long,
        profile: PostemberPlayerProfile
    ): CompletableFuture<Unit> =
        CompletableFuture.supplyAsync({ putPlayerProfile(namespaceId, rowId, profile); Unit }, executor)


    /**
     * Commits typed player profile upserts/removals as one EF7 CoW-tree update.
     * The JSONL bridge limits aggregate request size to one 64 KiB line.
     */
    fun applyPlayerProfileBatch(
        namespaceId: Long,
        upserts: List<PostemberPlayerProfileUpsert>,
        removals: List<PostemberPlayerRemoval>
    ): Long {
        check(!Bukkit.isPrimaryThread()) { "Use applyPlayerProfileBatchAsync() on Paper's main thread" }
        require(namespaceId > 0) { "namespaceId must be positive" }
        require(upserts.size + removals.size <= 2_500) { "EF7 profile batch is limited to 2,500 operations" }
        val tokens = ArrayList<String>(5 + upserts.size * 7 + removals.size * 2)
        tokens += "EF7"
        tokens += "PROFILE_BATCH"
        tokens += namespaceId.toString()
        tokens += upserts.size.toString()
        tokens += removals.size.toString()
        for (item in upserts) {
            require(item.rowId > 0) { "profile rowId must be positive" }
            require(item.profile.customData.size <= MAX_EF7_WIRE_CUSTOM_BYTES) {
                "EF7 JSONL/1 customData limit is $MAX_EF7_WIRE_CUSTOM_BYTES bytes per profile"
            }
            require(item.profile.username.isNotBlank() && item.profile.username.none { it.isWhitespace() }) {
                "username must be a single Minecraft player-name token"
            }
            tokens += item.rowId.toString()
            tokens += item.profile.uuid.toString().replace("-", "")
            tokens += item.profile.username
            tokens += item.profile.firstJoinedMs.toString()
            tokens += item.profile.lastSeenMs.toString()
            tokens += item.profile.playtimeTicks.toString()
            tokens += item.profile.customData.toLowerHex().ifEmpty { "-" }
        }
        for (item in removals) {
            require(item.rowId > 0) { "removed rowId must be positive" }
            tokens += item.rowId.toString()
            tokens += item.uuid.toString().replace("-", "")
        }
        val response = protocolRequestLine(tokens.joinToString(" "))
        return response["affected_rows"]?.jsonPrimitive?.longOrNull ?: 0L
    }

    fun applyPlayerProfileBatchAsync(
        namespaceId: Long,
        upserts: List<PostemberPlayerProfileUpsert>,
        removals: List<PostemberPlayerRemoval>
    ): CompletableFuture<Long> =
        CompletableFuture.supplyAsync({ applyPlayerProfileBatch(namespaceId, upserts, removals) }, executor)

    fun getPlayerProfile(
        namespaceId: Long,
        uuid: UUID
    ): PostemberStoredPlayerProfile? {
        check(!Bukkit.isPrimaryThread()) { "Use getPlayerProfileAsync() on Paper's main thread" }
        require(namespaceId > 0) { "namespaceId must be positive" }
        val response = protocolRequestLine(
            "EF7 PROFILE_GET $namespaceId ${uuid.toString().replace("-", "")}"
        )
        return response.readStoredProfile()
    }

    fun getPlayerProfileAsync(
        namespaceId: Long,
        uuid: UUID
    ): CompletableFuture<PostemberStoredPlayerProfile?> =
        CompletableFuture.supplyAsync({ getPlayerProfile(namespaceId, uuid) }, executor)

    fun getPlayerProfileByName(
        namespaceId: Long,
        username: String
    ): PostemberStoredPlayerProfile? {
        check(!Bukkit.isPrimaryThread()) { "Use getPlayerProfileByNameAsync() on Paper's main thread" }
        require(namespaceId > 0) { "namespaceId must be positive" }
        require(username.isNotBlank() && username.none { it.isWhitespace() }) {
            "username must be a single Minecraft player-name token"
        }
        return protocolRequestLine("EF7 PROFILE_GET_NAME $namespaceId $username").readStoredProfile()
    }

    fun getPlayerProfileByNameAsync(
        namespaceId: Long,
        username: String
    ): CompletableFuture<PostemberStoredPlayerProfile?> =
        CompletableFuture.supplyAsync({ getPlayerProfileByName(namespaceId, username) }, executor)

    fun deletePlayer(
        namespaceId: Long,
        rowId: Long,
        uuid: UUID
    ): Boolean {
        check(!Bukkit.isPrimaryThread()) { "Use deletePlayerAsync() on Paper's main thread" }
        require(namespaceId > 0 && rowId > 0) { "namespaceId and rowId must be positive" }
        val response = protocolRequestLine(
            "EF7 PLAYER_DELETE $namespaceId $rowId ${uuid.toString().replace("-", "")}"
        )
        return (response["affected_rows"] as? JsonPrimitive)?.longOrNull == 1L
    }

    fun deletePlayerAsync(
        namespaceId: Long,
        rowId: Long,
        uuid: UUID
    ): CompletableFuture<Boolean> =
        CompletableFuture.supplyAsync({ deletePlayer(namespaceId, rowId, uuid) }, executor)


    private data class BinaryResponse(val kind: Int, val requestId: Long, val payload: ByteArray)

    private fun binaryRequest(kind: Int, payload: ByteArray): BinaryResponse {
        check(!closed.get()) { "PostemberDB KSL client is closed" }
        require(payload.size <= MAX_BINARY_FRAME_PAYLOAD) { "Binary/1 payload exceeds the 8 MiB frame limit" }
        val address = InetAddress.getByName(host)
        require(address.isLoopbackAddress) { "PostemberDB connection must use loopback" }
        val requestId = nextBinaryRequestId.getAndIncrement()
        Socket().use { socket ->
            socket.tcpNoDelay = true
            socket.connect(InetSocketAddress(address, port), connectTimeoutMs)
            socket.soTimeout = readTimeoutMs
            val greeting = BufferedReader(InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
            check(!greeting.readLine().isNullOrBlank()) { "PostemberDB closed before greeting" }
            val writer = BufferedWriter(OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))
            writer.write("POSTEMBERDB BINARY/1\\n")
            writer.flush()
            val input = DataInputStream(socket.getInputStream())
            val output = DataOutputStream(socket.getOutputStream())
            val ready = readBinaryFrame(input)
            check(ready.kind == BINARY_PONG && ready.requestId == 0L &&
                ready.payload.toString(StandardCharsets.UTF_8) == "postemberdb-binary/1-ready") {
                "PostemberDB server did not acknowledge Binary/1"
            }
            writeBinaryFrame(output, kind, requestId, payload)
            val response = readBinaryFrame(input)
            if (response.requestId != requestId) throw PostemberDbException("binary response request ID mismatch")
            if (response.kind == BINARY_ERROR) {
                throw PostemberDbException(response.payload.toString(StandardCharsets.UTF_8))
            }
            return response
        }
    }

    private fun writeBinaryFrame(output: DataOutputStream, kind: Int, requestId: Long, payload: ByteArray) {
        val crc = CRC32().apply { update(payload) }.value.toInt()
        val header = ByteBuffer.allocate(BINARY_HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN)
            .put(byteArrayOf(0x50.toByte(), 0x44.toByte(), 0x42.toByte(), 0x46.toByte()))
            .put(BINARY_PROTOCOL_VERSION.toByte())
            .put(kind.toByte())
            .putShort(0)
            .putLong(requestId)
            .putInt(payload.size)
            .putInt(crc)
            .array()
        output.write(header)
        output.write(payload)
        output.flush()
    }

    private fun readBinaryFrame(input: DataInputStream): BinaryResponse {
        val header = ByteArray(BINARY_HEADER_BYTES)
        input.readFully(header)
        val decoded = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN)
        val magic = ByteArray(4)
        decoded.get(magic)
        if (!magic.contentEquals(byteArrayOf(0x50.toByte(), 0x44.toByte(), 0x42.toByte(), 0x46.toByte()))) {
            throw PostemberDbException("invalid Binary/1 frame magic")
        }
        if ((decoded.get().toInt() and 0xff) != BINARY_PROTOCOL_VERSION) {
            throw PostemberDbException("unsupported Binary/1 version")
        }
        val kind = decoded.get().toInt() and 0xff
        if (decoded.short.toInt() != 0) throw PostemberDbException("unsupported Binary/1 frame flags")
        val requestId = decoded.long
        val length = decoded.int
        val expectedCrc = decoded.int
        if (length < 0 || length > MAX_BINARY_FRAME_PAYLOAD) {
            throw PostemberDbException("Binary/1 frame length exceeds configured limit")
        }
        val payload = ByteArray(length)
        input.readFully(payload)
        val actualCrc = CRC32().apply { update(payload) }.value.toInt()
        if (actualCrc != expectedCrc) throw PostemberDbException("Binary/1 payload checksum mismatch")
        return BinaryResponse(kind, requestId, payload)
    }

    private fun JsonObject.toQueryResult(): PostemberQueryResult {
        if (this["ok"]?.jsonPrimitive?.boolean != true) {
            throw PostemberDbException(this["error"]?.jsonPrimitive?.contentOrNull ?: "binary SQL query failed")
        }
        return PostemberQueryResult(
            message = this["message"]?.jsonPrimitive?.contentOrNull ?: "OK",
            affectedRows = this["affected_rows"]?.jsonPrimitive?.longOrNull ?: 0L,
            columns = this["columns"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList(),
            rows = this["rows"]?.jsonArray?.map { row -> row.jsonArray.map { it.toNativeValue() } } ?: emptyList()
        )
    }

    private fun encodeSqlParameter(value: Any?): JsonObject = when (value) {
        null -> buildJsonObject { put("type", "null") }
        is String -> buildJsonObject {
            put("type", "text")
            put("value", JsonPrimitive(value))
        }
        is Boolean -> buildJsonObject {
            put("type", "boolean")
            put("value", JsonPrimitive(value))
        }
        is Byte, is Short, is Int, is Long -> buildJsonObject {
            put("type", "integer")
            put("value", JsonPrimitive((value as Number).toLong()))
        }
        else -> throw IllegalArgumentException(
            "Unsupported PostemberDB SQL parameter type ${value::class.qualifiedName}; use String, integral numbers, Boolean or null"
        )
    }

    private fun encodeSqlStatement(statement: PostemberSqlStatement): JsonObject {
        require(statement.sql.isNotBlank()) { "SQL statement cannot be blank" }
        require('\n' !in statement.sql && '\r' !in statement.sql) {
            "SQL statements must not contain line breaks"
        }
        require(statement.sql.toByteArray(StandardCharsets.UTF_8).size <= MAX_LINE_BYTES) {
            "SQL statement exceeds the JSONL/1 line limit"
        }
        return buildJsonObject {
            put("sql", statement.sql)
            put("parameters", JsonArray(statement.parameters.map(::encodeSqlParameter)))
        }
    }

    private fun parameterizedRequest(sql: String, parameters: List<Any?>): PostemberQueryResult {
        val statement = encodeSqlStatement(PostemberSqlStatement(sql, parameters))
        val response = protocolRequestLine("SQL_PARAMS $statement")
        return response.toQueryResult()
    }

    private fun batchRequest(statements: List<PostemberSqlStatement>): List<PostemberQueryResult> {
        require(statements.isNotEmpty() && statements.size <= MAX_SQL_BATCH_STATEMENTS) {
            "SQL transaction batch must contain 1..$MAX_SQL_BATCH_STATEMENTS statements"
        }
        val payload = buildJsonObject {
            put("statements", JsonArray(statements.map(::encodeSqlStatement)))
        }
        val response = protocolRequestLine("SQL_BATCH $payload")
        val results = response["results"]?.jsonArray
            ?: throw PostemberDbException("PostemberDB SQL batch response did not contain statement results")
        if (results.size != statements.size) {
            throw PostemberDbException("PostemberDB SQL batch returned ${results.size} results for ${statements.size} statements")
        }
        return results.map { element ->
            val result = element.jsonObject
            result.toQueryResult()
        }
    }

    private fun request(sql: String): PostemberQueryResult {
        require(sql.isNotBlank()) { "SQL statement cannot be blank" }
        val response = protocolRequestLine(sql)
        val columns = response["columns"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList()
        val rows = response["rows"]?.jsonArray?.map { row ->
            row.jsonArray.map { it.toNativeValue() }
        } ?: emptyList()
        return PostemberQueryResult(
            message = response["message"]?.jsonPrimitive?.contentOrNull ?: "OK",
            affectedRows = response["affected_rows"]?.jsonPrimitive?.longOrNull ?: 0L,
            columns = columns,
            rows = rows
        )
    }

    private fun connectJsonl(): PooledJsonlConnection {
        check(!closed.get()) { "PostemberDB KSL client is closed" }
        val address = InetAddress.getByName(host)
        require(address.isLoopbackAddress) { "PostemberDB connection must use loopback" }
        val socket = Socket()
        try {
            socket.tcpNoDelay = true
            socket.connect(InetSocketAddress(address, port), connectTimeoutMs)
            socket.soTimeout = readTimeoutMs
            val reader = BufferedReader(InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
            val writer = BufferedWriter(OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))
            check(!reader.readLine().isNullOrBlank()) { "PostemberDB closed before greeting" }
            writer.write("POSTEMBERDB JSONL/1\n")
            writer.flush()
            val helloLine = reader.readLine() ?: error("PostemberDB did not acknowledge JSONL/1")
            val hello = json.parseToJsonElement(helloLine).jsonObject
            check(hello["ready"]?.jsonPrimitive?.boolean == true && hello["version"]?.jsonPrimitive?.int == 1) {
                "PostemberDB server does not support JSONL/1"
            }
            return PooledJsonlConnection(socket, reader, writer).also(jsonlConnections::add)
        } catch (error: Exception) {
            try { socket.close() } catch (_: Exception) {}
            throw error
        }
    }

    private fun discardJsonlConnection(connection: PooledJsonlConnection) {
        idleJsonlConnections.remove(connection)
        jsonlConnections.remove(connection)
        connection.closeQuietly()
    }

    private fun protocolRequestLine(command: String): JsonObject {
        check(!closed.get()) { "PostemberDB KSL client is closed" }
        require(command.isNotBlank()) { "PostemberDB request cannot be blank" }
        require('\n' !in command && '\r' !in command) { "PostemberDB JSONL/1 accepts one command per line" }
        require(command.toByteArray(StandardCharsets.UTF_8).size <= MAX_LINE_BYTES) {
            "PostemberDB request exceeds 64 KiB"
        }

        val acquired = try {
            jsonlPermits.tryAcquire(connectTimeoutMs.toLong(), TimeUnit.MILLISECONDS)
        } catch (interrupted: InterruptedException) {
            Thread.currentThread().interrupt()
            throw PostemberDbException("Interrupted while waiting for a PostemberDB connection")
        }
        if (!acquired) {
            throw PostemberDbException("PostemberDB connection pool is busy; retry from an asynchronous worker")
        }

        var connection: PooledJsonlConnection? = null
        var reusable = false
        try {
            connection = idleJsonlConnections.poll()
            if (connection != null) {
                val idleNanos = System.nanoTime() - connection.lastUsedAtNanos
                if (connection.socket.isClosed || idleNanos >= JSONL_CONNECTION_MAX_IDLE_NANOS) {
                    discardJsonlConnection(connection)
                    connection = null
                }
            }
            if (connection == null) {
                connection = connectJsonl()
            }

            val active = connection
            active.writer.write(command)
            active.writer.write("\n")
            active.writer.flush()
            val responseLine = active.reader.readLine()
                ?: throw PostemberDbException("PostemberDB closed a pooled JSONL/1 connection")
            val response = json.parseToJsonElement(responseLine).jsonObject
            reusable = true
            return response
        } catch (error: Exception) {
            throw if (error is PostemberDbException) error
                else PostemberDbException("PostemberDB JSONL/1 request failed: " + (error.message ?: error.javaClass.simpleName))
        } finally {
            val usedConnection = connection
            if (usedConnection != null) {
                if (reusable && !closed.get() && !usedConnection.socket.isClosed) {
                    usedConnection.lastUsedAtNanos = System.nanoTime()
                    if (!idleJsonlConnections.offer(usedConnection)) {
                        discardJsonlConnection(usedConnection)
                    }
                } else {
                    discardJsonlConnection(usedConnection)
                }
            }
            jsonlPermits.release()
        }
    }

    private fun JsonObject.readNamespace(): PostemberNamespace? {
        val element = this["namespace"] ?: return null
        if (element == JsonNull) return null
        return element.jsonObject.toNamespace()
    }

    private fun JsonObject.toNamespace(): PostemberNamespace {
        val parentElement = this["parent_id"]
        val parentId = if (parentElement == null || parentElement == JsonNull) {
            null
        } else {
            parentElement.jsonPrimitive.longOrNull
                ?: throw PostemberDbException("EF7 namespace response contains invalid parent_id")
        }
        return PostemberNamespace(
            id = this["id"]?.jsonPrimitive?.longOrNull
                ?: throw PostemberDbException("EF7 namespace response omitted id"),
            parentId = parentId,
            kind = this["kind"]?.jsonPrimitive?.content
                ?: throw PostemberDbException("EF7 namespace response omitted kind"),
            name = this["name"]?.jsonPrimitive?.content
                ?: throw PostemberDbException("EF7 namespace response omitted name")
        )
    }

    private fun JsonObject.readRecord(): PostemberStoredRecord? {
        val element = this["record"] ?: return null
        if (element == JsonNull) return null
        val record = element.jsonObject
        val payloadHex = record["payload_hex"]?.jsonPrimitive?.content
            ?: throw PostemberDbException("EF7 record response omitted payload_hex")
        return PostemberStoredRecord(
            namespaceId = record["namespace_id"]?.jsonPrimitive?.longOrNull
                ?: throw PostemberDbException("EF7 record response omitted namespace_id"),
            recordId = record["record_id"]?.jsonPrimitive?.longOrNull
                ?: throw PostemberDbException("EF7 record response omitted record_id"),
            payload = payloadHex.decodeHex()
        )
    }

    private fun JsonObject.readChunk(): PostemberChunk? {
        val element = this["chunk"] ?: return null
        if (element == JsonNull) return null
        val chunk = element.jsonObject
        val payloadHex = chunk["payload_hex"]?.jsonPrimitive?.content
            ?: throw PostemberDbException("EF7 chunk response omitted payload_hex")
        return PostemberChunk(
            namespaceId = chunk["namespace_id"]?.jsonPrimitive?.longOrNull
                ?: throw PostemberDbException("EF7 chunk response omitted namespace_id"),
            chunkX = chunk["chunk_x"]?.jsonPrimitive?.intOrNull
                ?: throw PostemberDbException("EF7 chunk response omitted chunk_x"),
            chunkZ = chunk["chunk_z"]?.jsonPrimitive?.intOrNull
                ?: throw PostemberDbException("EF7 chunk response omitted chunk_z"),
            payload = payloadHex.decodeHex()
        )
    }

    private fun JsonObject.readStoredProfile(): PostemberStoredPlayerProfile? {
        val element = this["profile"] ?: return null
        if (element == JsonNull) return null
        val profile = element.jsonObject
        val uuidHex = profile["uuid"]?.jsonPrimitive?.content
            ?: throw PostemberDbException("EF7 response omitted player UUID")
        if (uuidHex.length != 32 || uuidHex.any { it !in "0123456789abcdefABCDEF" }) {
            throw PostemberDbException("EF7 response contains an invalid player UUID")
        }
        val uuid = UUID.fromString(
            uuidHex.substring(0, 8) + "-" + uuidHex.substring(8, 12) + "-" +
                uuidHex.substring(12, 16) + "-" + uuidHex.substring(16, 20) + "-" + uuidHex.substring(20)
        )
        val username = profile["username"]?.jsonPrimitive?.content
            ?: throw PostemberDbException("EF7 response omitted player username")
        val customHex = profile["custom_data_hex"]?.jsonPrimitive?.content ?: ""
        return PostemberStoredPlayerProfile(
            rowId = profile["row_id"]?.jsonPrimitive?.longOrNull
                ?: throw PostemberDbException("EF7 response omitted row_id"),
            profile = PostemberPlayerProfile(
                uuid = uuid,
                username = username,
                firstJoinedMs = profile["first_joined_ms"]?.jsonPrimitive?.longOrNull ?: throw PostemberDbException("EF7 response omitted first_joined_ms"),
                lastSeenMs = profile["last_seen_ms"]?.jsonPrimitive?.longOrNull ?: throw PostemberDbException("EF7 response omitted last_seen_ms"),
                playtimeTicks = profile["playtime_ticks"]?.jsonPrimitive?.longOrNull ?: throw PostemberDbException("EF7 response omitted playtime_ticks"),
                customData = customHex.decodeHex()
            )
        )
    }


    override fun close() {
        if (closed.compareAndSet(false, true)) {
            jsonlConnections.toList().forEach(::discardJsonlConnection)
            idleJsonlConnections.clear()
            executor.shutdownNow()
        }
    }
}

private const val MAX_LINE_BYTES = 64 * 1024
private const val MAX_SQL_BATCH_STATEMENTS = 500
private val JSONL_CONNECTION_MAX_IDLE_NANOS = TimeUnit.SECONDS.toNanos(25)
private const val BINARY_HEADER_BYTES = 24
private const val BINARY_PROTOCOL_VERSION = 1
private const val MAX_BINARY_FRAME_PAYLOAD = 8 * 1024 * 1024
private const val BINARY_QUERY = 1
private const val BINARY_QUERY_RESULT = 2
private const val BINARY_ERROR = 3
private const val BINARY_PING = 4
private const val BINARY_PONG = 5
private const val BINARY_CHUNK_PUT = 16
private const val BINARY_CHUNK_GET = 17
private const val BINARY_CHUNK_DELETE = 18
private const val BINARY_CHUNK_DATA = 19
private const val MAX_EF7_WIRE_CUSTOM_BYTES = 24 * 1024
private const val MAX_EF7_WIRE_CHUNK_BYTES = 24 * 1024
private const val MAX_EF7_WIRE_RECORD_BYTES = 24 * 1024

private fun ByteArray.toLowerHex(): String {
    val alphabet = "0123456789abcdef"
    val output = CharArray(size * 2)
    for (index in indices) {
        val value = this[index].toInt() and 0xff
        output[index * 2] = alphabet[value ushr 4]
        output[index * 2 + 1] = alphabet[value and 0x0f]
    }
    return String(output)
}

private fun String.decodeHex(): ByteArray {
    require(length % 2 == 0 && all { it in "0123456789abcdefABCDEF" }) { "invalid hex value from EF7" }
    return ByteArray(length / 2) { index ->
        substring(index * 2, index * 2 + 2).toInt(16).toByte()
    }
}

private fun JsonElement.toNativeValue(): Any? = when (this) {
    JsonNull -> null
    is JsonObject, is JsonArray -> toString()
    is JsonPrimitive -> when {
        isString -> content
        booleanOrNull != null -> boolean
        longOrNull != null -> long
        doubleOrNull != null -> double
        else -> content
    }
}

class PostemberDbException(message: String) : RuntimeException(message)

fun postemberDb(): PostemberDbClient =
    ru.privateserver.ksl.KSL.api.getService("postemberdb", PostemberDbClient::class.java)
        ?: error("PostemberDB service is unavailable; check that PostemberDBKSL is enabled")

/** Quote a SQL text literal; keep identifiers static and trusted. */
fun postemberText(value: String): String = "'" + value.replace("'", "''") + "'"
