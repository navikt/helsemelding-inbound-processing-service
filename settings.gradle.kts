dependencyResolutionManagement {

    versionCatalogs {
        create("libs") {
            version("arrow", "2.0.1")
            version("suspendapp", "0.5.0")
            version("ktor", "3.0.3")
            version("kotlin-logging", "7.0.3")
            version("kotlinx-serialization", "1.9.0")
            version("kafka-streams", "4.2.0")
            version("hoplite", "2.8.2")
            version("prometheus", "1.12.4")
            version("logback", "1.4.11")
            version("logstash", "7.4")
            version("opentelemetry-mdc", "2.24.0-alpha")
            version("opentelemetry-extension-kotlin", "1.58.0")

            library("arrow-core", "io.arrow-kt", "arrow-core").versionRef("arrow")
            library("arrow-functions", "io.arrow-kt", "arrow-functions").versionRef("arrow")
            library("arrow-fx-coroutines", "io.arrow-kt", "arrow-fx-coroutines").versionRef("arrow")
            library("arrow-suspendapp", "io.arrow-kt", "suspendapp").versionRef("suspendapp")
            library("arrow-suspendapp-ktor", "io.arrow-kt", "suspendapp-ktor").versionRef("suspendapp")

            library("hoplite-core", "com.sksamuel.hoplite", "hoplite-core").versionRef("hoplite")
            library("hoplite-hocon", "com.sksamuel.hoplite", "hoplite-hocon").versionRef("hoplite")

            library("ktor-server-core", "io.ktor", "ktor-server-core").versionRef("ktor")
            library("ktor-server-netty", "io.ktor", "ktor-server-netty").versionRef("ktor")

            library("ktor-server-metrics-micrometer", "io.ktor", "ktor-server-metrics-micrometer").versionRef("ktor")
            library("micrometer-registry-prometheus", "io.micrometer", "micrometer-registry-prometheus").versionRef("prometheus")

            library("kafka-streams", "org.apache.kafka", "kafka-streams").versionRef("kafka-streams")

            library("kotlinx-serialization-json", "org.jetbrains.kotlinx", "kotlinx-serialization-json").versionRef("kotlinx-serialization")

            library("kotlin-logging", "io.github.oshai", "kotlin-logging-jvm").versionRef("kotlin-logging")
            library("logback-classic", "ch.qos.logback", "logback-classic").versionRef("logback")
            library("logback-logstash", "net.logstash.logback", "logstash-logback-encoder").versionRef("logstash")

            library("opentelemetry-logback-mdc", "io.opentelemetry.instrumentation", "opentelemetry-logback-mdc-1.0").versionRef("opentelemetry-mdc")
            library("opentelemetry-extension-kotlin", "io.opentelemetry", "opentelemetry-extension-kotlin").versionRef("opentelemetry-extension-kotlin")

            bundle("prometheus", listOf("ktor-server-metrics-micrometer", "micrometer-registry-prometheus"))
            bundle("logging", listOf("logback-classic", "logback-logstash"))
            bundle("opentelemetry", listOf("opentelemetry-logback-mdc", "opentelemetry-extension-kotlin"))
        }

        create("testLibs") {
            version("ktor-server-test", "3.0.3")
            version("kotest", "5.9.1")
            version("kafka-streams", "4.2.0")
            version("mockk", "1.14.9")

            library("ktor-server-test-host", "io.ktor", "ktor-server-test-host").versionRef("ktor-server-test")

            library("kotest-runner-junit5", "io.kotest", "kotest-runner-junit5").versionRef("kotest")
            library("kotest-framework-datatest", "io.kotest", "kotest-framework-datatest").versionRef("kotest")

            library("kafka-streams", "org.apache.kafka", "kafka-streams-test-utils").versionRef("kafka-streams")

            library("mockk", "io.mockk", "mockk").versionRef("mockk")

            bundle("kotest", listOf("kotest-runner-junit5", "kotest-framework-datatest"))
        }
    }

    repositories {
        mavenCentral()
    }
}

rootProject.name = "helsemelding-inbound-processing-service"
