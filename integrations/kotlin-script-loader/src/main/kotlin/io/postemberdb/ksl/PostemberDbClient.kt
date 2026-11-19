package io.postemberdb.ksl

import kotlinx.serialization.json.*
import org.bukkit.Bukkit
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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID

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

/** Thread-safe loopback bridge to the PostemberDB JSONL/1 protocol. */
class PostemberDbClient internal constructor(
    private val host: String,
    private val port: Int,
    private val connectTimeoutMs: Int,
    private val readTimeoutMs: Int,
    workerThreads: Int
) : Closeable {
    private val closed = AtomicBoolean(false)
    private val workerCount = workerThreads.coerceIn(1, 8)
    private val executor: ExecutorService = ThreadPoolExecutor(
        workerCount, workerCount, 0L, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(256), ThreadPoolExecutor.AbortPolicy()
    )
    private val json = Json { ignoreUnknownKeys = true }

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

    fun executeAsync(sql: String): CompletableFuture<Long> =
        queryAsync(sql).thenApply { it.affectedRows }


    fun putPlayerProfile(
        namespaceId: Long,
        rowId: Long,
        profile: PostemberPlayerProfile
    ) {
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

    private fun protocolRequestLine(command: String): JsonObject {
        check(!closed.get()) { "PostemberDB KSL client is closed" }
        require(command.isNotBlank()) { "PostemberDB request cannot be blank" }
        require('\n' !in command && '\r' !in command) { "PostemberDB JSONL/1 accepts one command per line" }
        require(command.toByteArray(StandardCharsets.UTF_8).size <= MAX_LINE_BYTES) {
            "PostemberDB request exceeds 64 KiB"
        }
        val address = InetAddress.getByName(host)
        require(address.isLoopbackAddress) { "PostemberDB connection must use loopback" }

        Socket().use { socket ->
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
            writer.write(command)
            writer.write("\n")
            writer.flush()
            val responseLine = reader.readLine() ?: error("PostemberDB returned an empty response")
            val response = json.parseToJsonElement(responseLine).jsonObject
            if (response["ok"]?.jsonPrimitive?.boolean != true) {
                throw PostemberDbException(response["error"]?.jsonPrimitive?.contentOrNull ?: "unknown database error")
            }
            return response
        }
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
        if (closed.compareAndSet(false, true)) executor.shutdownNow()
    }
}

private const val MAX_LINE_BYTES = 64 * 1024
private const val MAX_EF7_WIRE_CUSTOM_BYTES = 24 * 1024

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
