package no.nav.helsemelding.inbound.processing.stream

import arrow.core.Either
import arrow.core.getOrElse
import no.nav.helsemelding.inbound.processing.model.ErrorCategory
import no.nav.helsemelding.inbound.processing.model.ErrorCode
import no.nav.helsemelding.inbound.processing.model.ProcessingError
import kotlin.uuid.Uuid

data class InboundMessageValidation(
    val recordKey: RecordKeyValidation,
    val recordValue: RecordValueValidation,
    val recordMetadata: RecordMetadataValidation
)

fun InboundMessageValidation.isValid(): Boolean =
    recordKey.isValid &&
        recordValue.isValid &&
        recordMetadata.isValid

fun InboundMessageValidation.errors(): List<ProcessingError> =
    buildList {
        when (val key = recordKey) {
            is RecordKeyValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_KAFKA_KEY,
                        message = key.reason
                    )
                )

            RecordKeyValidation.Valid -> Unit
        }

        when (val value = recordValue) {
            is RecordValueValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_KAFKA_VALUE,
                        message = value.reason
                    )
                )

            RecordValueValidation.Valid -> Unit
        }

        when (val metadata = recordMetadata) {
            is RecordMetadataValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.MISSING_SOURCE_SYSTEM_HEADER,
                        message = metadata.reason
                    )
                )

            RecordMetadataValidation.Valid -> Unit
        }
    }

class InboundMessageValidator {
    fun validate(
        key: String?,
        value: String?,
        sourceSystem: String?
    ): InboundMessageValidation =
        InboundMessageValidation(
            recordKey = validateRecordKey(key),
            recordValue = validateRecordValue(value),
            recordMetadata = validateRecordMetadata(sourceSystem)
        )
}

sealed interface Validation {
    val isValid: Boolean
}

sealed interface RecordKeyValidation : Validation {
    data object Valid : RecordKeyValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordKeyValidation {
        override val isValid = false
    }
}

internal fun validateRecordKey(
    key: String?
): RecordKeyValidation =
    when {
        key == null ->
            RecordKeyValidation.Invalid(
                "Kafka record key is null"
            )

        Uuid.parseOrNull(key) == null ->
            RecordKeyValidation.Invalid(
                "Kafka record key is not a valid UUID"
            )

        else -> RecordKeyValidation.Valid
    }

sealed interface RecordValueValidation : Validation {
    data object Valid : RecordValueValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordValueValidation {
        override val isValid = false
    }
}

// TODO: Validate XML schema
internal fun validateRecordValue(
    value: String?
): RecordValueValidation =
    when {
        value == null ->
            RecordValueValidation.Invalid(
                "Kafka record value is null"
            )

        value.isEmpty() ->
            RecordValueValidation.Invalid(
                "Kafka record value is empty"
            )

        !value.isValidXml() ->
            RecordValueValidation.Invalid(
                "Kafka record value is not valid XML"
            )

        else -> RecordValueValidation.Valid
    }

// TODO: Implement proper XML validation
private fun String.isValidXml(): Boolean =
    Either.catch {
        javax.xml.parsers.DocumentBuilderFactory.newInstance()
            .newDocumentBuilder()
            .parse(org.xml.sax.InputSource(java.io.StringReader(this)))
        true
    }
        .getOrElse { false }

sealed interface RecordMetadataValidation : Validation {
    data object Valid : RecordMetadataValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : RecordMetadataValidation {
        override val isValid = false
    }
}

internal fun validateRecordMetadata(sourceSystem: String?): RecordMetadataValidation {
    return when {
        sourceSystem.isNullOrBlank() ->
            RecordMetadataValidation.Invalid(
                "Kafka record header '$SOURCE_SYSTEM_HEADER' is missing or empty"
            )

        else -> RecordMetadataValidation.Valid
    }
}
