package no.nav.helsemelding.inbound.processing.stream

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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
            .foreach { key, value ->
                log.warn {
                    val errors = value.error().errors
                        .joinToString { error ->
                            "${error.code}: ${error.message}"
                        }
                    "Message rejected by inbound validation: key=$key errors=[$errors]"
                }
            }
    }

    private fun KStream<String, ProcessedMessage>.toJsonPayload(): KStream<String, String> =
        flatMapValues { message ->
            messageConverter.incomingDialogMessageXmlToJson(message.payload)
                .fold(
                    {
                        throw FatalConversionException("Failed to convert XML to JSON: ${it.message}")
                    },
                    { convertedJson ->
                        convertedJson.withAttachmentCount(message.attachmentCount)
                            ?.let(::listOf)
                            ?: emptyList()
                    }
                )
        }

    private fun String.withAttachmentCount(attachmentCount: Int?): String? {
        val jsonObject = try {
            Json.parseToJsonElement(this) as JsonObject
        } catch (_: SerializationException) {
            log.error { "Converted JSON is invalid" }
            return null
        } catch (_: IllegalArgumentException) {
            log.error { "Converted JSON is invalid" }
            return null
        }

        if (!jsonObject.containsKey("numberOfAttachments")) {
            log.error { "Converted JSON is missing numberOfAttachments" }
            return null
        }

        return JsonObject(
            jsonObject + ("numberOfAttachments" to JsonPrimitive(requireNotNull(attachmentCount)))
        ).toString()
    }
}

internal class FatalConversionException(message: String) : RuntimeException(message)
