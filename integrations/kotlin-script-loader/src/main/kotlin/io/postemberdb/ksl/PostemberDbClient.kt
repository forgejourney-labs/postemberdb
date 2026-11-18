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

    private fun request(sql: String): PostemberQueryResult {
        check(!closed.get()) { "PostemberDB KSL client is closed" }
        require(sql.isNotBlank()) { "SQL statement cannot be blank" }
        require(sql.toByteArray(StandardCharsets.UTF_8).size <= 64 * 1024) { "SQL statement exceeds 64 KiB" }
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
            writer.write(sql)
            writer.write("\n")
            writer.flush()
            val responseLine = reader.readLine() ?: error("PostemberDB returned an empty response")
            val response = json.parseToJsonElement(responseLine).jsonObject
            if (response["ok"]?.jsonPrimitive?.boolean != true) {
                throw PostemberDbException(response["error"]?.jsonPrimitive?.contentOrNull ?: "unknown database error")
            }
            val columns = response["columns"]?.jsonArray?.map { it.jsonPrimitive.content } ?: emptyList()
            val rows = response["rows"]?.jsonArray?.map { row -> row.jsonArray.map { it.toNativeValue() } } ?: emptyList()
            return PostemberQueryResult(
                message = response["message"]?.jsonPrimitive?.contentOrNull ?: "OK",
                affectedRows = response["affected_rows"]?.jsonPrimitive?.longOrNull ?: 0L,
                columns = columns,
                rows = rows
            )
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) executor.shutdownNow()
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
