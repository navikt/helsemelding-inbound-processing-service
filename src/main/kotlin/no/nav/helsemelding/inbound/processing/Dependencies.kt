package no.nav.helsemelding.inbound.processing

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.await.awaitAll
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.prometheus.PrometheusConfig.DEFAULT
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.helsemelding.inbound.processing.config.KafkaStreamsSettings
import no.nav.helsemelding.inbound.processing.stream.InboundMessageTopology
import no.nav.helsemelding.inbound.processing.stream.InboundMessageValidator
import org.apache.kafka.streams.KafkaStreams

private val log = KotlinLogging.logger {}

data class Dependencies(
    val meterRegistry: PrometheusMeterRegistry,
    val kafkaStreams: KafkaStreams
)

internal suspend fun ResourceScope.metricsRegistry(): PrometheusMeterRegistry =
    install({ PrometheusMeterRegistry(DEFAULT) }) { p, _: ExitCase ->
        p.close().also { log.info { "Closed Prometheus registry" } }
    }

internal suspend fun ResourceScope.kafkaStreams(kafkaStreamsSettings: KafkaStreamsSettings): KafkaStreams =
    install({ kafkaStreamsSettings.toKafkaStreams(inboundMessageTopology()).apply { start() } }) { ks, _: ExitCase ->
        ks.close().also { log.info { "Closed Kafka Streams" } }
    }

suspend fun ResourceScope.dependencies(): Dependencies = awaitAll {
    val config = config()

    val metricsRegistry = async { metricsRegistry() }
    val kafkaStreams = async { kafkaStreams(config.kafkaStreamsSettings) }

    Dependencies(
        metricsRegistry.await(),
        kafkaStreams.await()
    )
}

private fun inboundMessageTopology(): InboundMessageTopology =
    InboundMessageTopology(
        InboundMessageValidator()
    )
