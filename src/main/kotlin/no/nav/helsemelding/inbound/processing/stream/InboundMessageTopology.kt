package no.nav.helsemelding.inbound.processing.stream

import arrow.core.getOrElse
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.Json
import no.nav.helsemelding.inbound.processing.config
import no.nav.helsemelding.messageconverter.MessageConverter
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier

private val log = KotlinLogging.logger {}

class InboundMessageTopology(
    private val validator: InboundMessageValidator,
    private val messageConverter: MessageConverter
) {
    fun build(): Topology {
        val builder = StreamsBuilder()

        val processed = builder.processInboundMessages()

        processed.routeValidMessages()
        processed.routeInvalidMessages()

        return builder.build()
    }

    private fun StreamsBuilder.processInboundMessages(): KStream<String, ProcessedMessage> =
        stream<String, String>(config().kafkaStreamsSettings.topics.dialogMessageIn)
            .peek { key, value ->
                log.info {
                    "Received message: key=$key value=$value"
                }
            }
            .processValues(
                FixedKeyProcessorSupplier {
                    InboundMessageProcessor(validator)
                }
            )

    private fun KStream<String, ProcessedMessage>.routeValidMessages() {
        filter { _, value -> value.isValid() }
            .peek { key, value ->
                log.info {
                    "Message passed inbound validation: key=$key payload=${value.payload}"
                }
            }
            .toJsonPayload()
            .to(config().kafkaStreamsSettings.topics.dialogMessageOut)
    }

    private fun KStream<String, ProcessedMessage>.routeInvalidMessages() {
        filterNot { _, value -> value.isValid() }
            .peek { key, value ->
                log.warn {
                    val errors = value.error().errors
                        .joinToString { error ->
                            "${error.code}: ${error.message}"
                        }
                    "Message rejected by inbound validation: key=$key errors=[$errors]"
                }
            }
            .mapValues(ProcessedMessage::error)
            .mapValues { errorMessage ->
                Json.encodeToString(errorMessage)
            }
            .to(config().kafkaStreamsSettings.topics.dialogMessageError)
    }

    private fun KStream<String, ProcessedMessage>.toJsonPayload(): KStream<String, String> =
        mapValues { message ->
            messageConverter.incomingDialogMessageXmlToJson(message.payload)
                .getOrElse { error ->
                    throw RuntimeException("Failed to convert XML to JSON: ${error.message}", error.cause)
                }
        }
}
