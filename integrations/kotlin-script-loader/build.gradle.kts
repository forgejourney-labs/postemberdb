plugins {
    kotlin("jvm") version "2.0.21"
}

group = "io.postemberdb"
version = "0.1.0"

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.21.1-R0.1-SNAPSHOT")
    compileOnly(files("libs/KotlinScriptLoader.jar"))
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
}

kotlin { jvmToolchain(21) }
java { toolchain.languageVersion.set(JavaLanguageVersion.of(21)) }

tasks.jar { archiveBaseName.set("PostemberDB-KSL") }

tasks.processResources {
    filesMatching("plugin.yml") {
        expand("version" to project.version)
    }
}
