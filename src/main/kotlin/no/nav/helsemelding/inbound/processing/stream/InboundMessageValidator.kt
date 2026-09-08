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
    val attachmentCount: AttachmentCountValidation
)

fun InboundMessageValidation.isValid(): Boolean =
    recordKey.isValid &&
        recordValue.isValid &&
        attachmentCount.isValid

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

        when (val attachmentCount = attachmentCount) {
            is AttachmentCountValidation.Invalid ->
                add(
                    ProcessingError(
                        category = ErrorCategory.VALIDATION,
                        code = ErrorCode.INVALID_ATTACHMENT_COUNT_HEADER,
                        message = attachmentCount.reason
                    )
                )

            is AttachmentCountValidation.Valid -> Unit
        }
    }

class InboundMessageValidator {
    fun validate(
        key: String?,
        value: String?,
        sourceSystem: String?,
        attachmentCount: String?
    ): InboundMessageValidation =
        InboundMessageValidation(
            recordKey = validateRecordKey(key),
            recordValue = validateRecordValue(value),
            attachmentCount = validateAttachmentCount(attachmentCount)
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

sealed interface AttachmentCountValidation : Validation {
    data class Valid(
        val value: Int
    ) : AttachmentCountValidation {
        override val isValid = true
    }

    data class Invalid(
        val reason: String
    ) : AttachmentCountValidation {
        override val isValid = false
    }
}

internal fun validateAttachmentCount(
    value: String?
): AttachmentCountValidation {
    if (value == null) {
        return AttachmentCountValidation.Invalid(
            "Kafka header attachments-count is missing"
        )
    }

    val attachmentCount = value.toIntOrNull()
        ?: return AttachmentCountValidation.Invalid(
            "Kafka header attachments-count is not a valid integer"
        )

    return AttachmentCountValidation.Valid(attachmentCount)
}
