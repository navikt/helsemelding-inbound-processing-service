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
                attachmentCount = AttachmentCountValidation.Valid(0)
            )

            validation.errors() shouldBe listOf(
                ProcessingError(
                    category = ErrorCategory.VALIDATION,
                    code = ErrorCode.INVALID_KAFKA_KEY,
                    message = "Kafka record key is not a valid UUID"
                )
            )
        }

        "should be valid when all validation results are valid" {
            val validation = InboundMessageValidation(
                recordKey = RecordKeyValidation.Valid,
                recordValue = RecordValueValidation.Valid,
                attachmentCount = AttachmentCountValidation.Valid(0)
            )

            validation.isValid() shouldBe true
        }

        "should accept an attachment count that can be converted to an integer" {
            validateAttachmentCount("5") shouldBe AttachmentCountValidation.Valid(5)
        }

        "should reject an attachment count that cannot be converted to an integer" {
            validateAttachmentCount("one").isValid shouldBe false
        }

        "should reject a missing attachment count" {
            validateAttachmentCount(null).isValid shouldBe false
        }
    }
)
