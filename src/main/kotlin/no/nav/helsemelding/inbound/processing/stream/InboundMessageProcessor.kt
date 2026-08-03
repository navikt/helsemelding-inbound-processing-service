package no.nav.helsemelding.inbound.processing.stream

import no.nav.helsemelding.inbound.processing.model.ErrorMessage
import no.nav.helsemelding.inbound.processing.model.OriginalMessage
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.time.Clock
import kotlin.time.Instant

internal const val SOURCE_SYSTEM_HEADER = "sourceSystem"

data class ProcessedMessage(
    val key: String?,
    val payload: String,
    val sourceSystem: String,
    val createdAt: Instant,
    val processedAt: Instant,
    val validation: InboundMessageValidation
) {
    fun isValid(): Boolean = validation.isValid()

    fun error(): ErrorMessage =
        ErrorMessage(
            processedAt = processedAt,
            sourceSystem = sourceSystem,
            errors = validation.errors(),
            originalMessage = OriginalMessage(
                createdAt = createdAt,
                key = key.orEmpty(),
                payload = payload
            )
        )
}

class InboundMessageProcessor(
    private val validator: InboundMessageValidator
) : FixedKeyProcessor<String, String, ProcessedMessage> {
    private lateinit var context: FixedKeyProcessorContext<String, ProcessedMessage>

    override fun init(context: FixedKeyProcessorContext<String, ProcessedMessage>) {
        this.context = context
    }

    override fun process(record: FixedKeyRecord<String, String>) {
        val sourceSystem = record.headers()
            .lastHeader(SOURCE_SYSTEM_HEADER)
            ?.value()
            ?.decodeToString()

        val validation = validator.validate(
            key = record.key(),
            value = record.value(),
            sourceSystem = sourceSystem
        )

        context.forward(
            record.withValue(
                ProcessedMessage(
                    key = record.key(),
                    payload = record.value(),
                    sourceSystem = sourceSystem ?: "UNKNOWN",
                    validation = validation,
                    createdAt = Instant.fromEpochMilliseconds(record.timestamp()),
                    processedAt = Clock.System.now()
                )
            )
        )
    }

    override fun close() {}
}
