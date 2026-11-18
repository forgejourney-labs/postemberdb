package io.postemberdb.ksl

import org.bukkit.plugin.java.JavaPlugin
import ru.privateserver.ksl.KSL
import ru.privateserver.ksl.KSLAPI
import ru.privateserver.ksl.KSLAddon

class PostemberDbAddon : JavaPlugin(), KSLAddon {
    override val addonId = "postemberdb-ksl"
    override val addonVersion = "0.1.0"
    override val addonDescription = "KSL service bridge for the PostemberDB Rust engine"
    override val minApiVersion = 16

    private lateinit var client: PostemberDbClient

    override fun onEnable() {
        if (!KSL.isAvailable) {
            logger.severe("KotlinScriptLoader is not available; disabling PostemberDB KSL addon")
            server.pluginManager.disablePlugin(this)
            return
        }
        val config = KSL.api.createConfig(this)
        val host = config.getString("database.host", "127.0.0.1") ?: "127.0.0.1"
        val port = config.getInt("database.port", 7432)
        val connectTimeout = config.getInt("database.connect-timeout-ms", 2000)
        val readTimeout = config.getInt("database.read-timeout-ms", 10000)
        val workers = config.getInt("database.worker-threads", 2)
        client = PostemberDbClient(host, port, connectTimeout, readTimeout, workers)
        KSL.api.registerAddon(this)
    }

    override fun onLoad(api: KSLAPI) {
        api.registerService("postemberdb", client)
        api.addDefaultImports("io.postemberdb.ksl.*")
        logger.info("PostemberDB service registered for KSL scripts")
    }

    override fun onUnload() {
        if (::client.isInitialized) client.close()
    }

    override fun onDisable() {
        if (KSL.isAvailable) KSL.api.unregisterAddon(addonId)
        if (::client.isInitialized) client.close()
    }
}
