package no.nav.helsemelding.inbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import no.nav.helsemelding.inbound.processing.model.ErrorCategory
import no.nav.helsemelding.inbound.processing.model.ErrorCode
import no.nav.helsemelding.inbound.processing.model.ProcessingError

class InboundMessageValidatorSpec : StringSpec(
    {
        "should map validation failures to processing errors" {
            val validation = InboundMessageValidation(
                recordKey = RecordKeyValidation.Invalid("Kafka record key is not a valid UUID"),
                recordValue = RecordValueValidation.Valid,
                recordMetadata = RecordMetadataValidation.Invalid(
                    "Kafka record header 'sourcesystem' is missing"
                )
            )

            validation.errors() shouldBe listOf(
                ProcessingError(
                    category = ErrorCategory.VALIDATION,
                    code = ErrorCode.INVALID_KAFKA_KEY,
                    message = "Kafka record key is not a valid UUID"
                ),
                ProcessingError(
                    category = ErrorCategory.VALIDATION,
                    code = ErrorCode.MISSING_SOURCE_SYSTEM_HEADER,
                    message = "Kafka record header 'sourcesystem' is missing"
                )
            )
        }

        "should be valid when all validation results are valid" {
            val validation = InboundMessageValidation(
                recordKey = RecordKeyValidation.Valid,
                recordValue = RecordValueValidation.Valid,
                recordMetadata = RecordMetadataValidation.Valid
            )

            validation.isValid() shouldBe true
        }
    }
)
