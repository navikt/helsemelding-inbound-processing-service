package no.nav.helsemelding.inbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class RecordValueValidationSpec : StringSpec(
    {
        "should return invalid when value is null" {
            validateRecordValue(null) shouldBe RecordValueValidation.Invalid(
                "Kafka record value is null"
            )
        }

        "should return invalid when value is empty" {
            validateRecordValue(String()) shouldBe RecordValueValidation.Invalid(
                "Kafka record value is empty"
            )
        }

        "should return invalid when value is not valid xml" {
            validateRecordValue("not valid xml") shouldBe
                RecordValueValidation.Invalid("Kafka record value is not valid XML")
        }

        "should return valid when value is valid xml" {
            validateRecordValue("<message><content>hello</content></message>") shouldBe
                RecordValueValidation.Valid
        }
    }
)
