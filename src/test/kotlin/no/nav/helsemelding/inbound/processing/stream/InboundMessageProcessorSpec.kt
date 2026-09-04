package no.nav.helsemelding.inbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import no.nav.helsemelding.inbound.processing.model.ErrorCode
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyRecord
import kotlin.uuid.Uuid

class InboundMessageProcessorSpec : StringSpec(
    {
        "should validate and forward processed message" {
            val key = Uuid.random().toString()
            val payload = "<message><content>hello</content></message>"
            val headers = RecordHeaders()
                .add("sourceSystem", "some-system".encodeToByteArray())

            val record = mockk<FixedKeyRecord<String, String>> {
                every { key() } returns key
                every { value() } returns payload
                every { timestamp() } returns 123456789L
                every { headers() } returns headers
                every { withValue(any<ProcessedMessage>()) } answers {
                    mockk<FixedKeyRecord<String, ProcessedMessage>> {
                        every { value() } returns firstArg()
                    }
                }
            }

            val context = mockk<FixedKeyProcessorContext<String, ProcessedMessage>>(relaxed = true)

            InboundMessageProcessor(InboundMessageValidator()).apply {
                init(context)
                process(record)
            }

            val forwarded = slot<FixedKeyRecord<String, ProcessedMessage>>()

            verify(exactly = 1) {
                context.forward(capture(forwarded))
            }

            forwarded.captured.value().apply {
                this.key shouldBe key
                this.payload shouldBe payload
                this.validation.isValid() shouldBe true
            }
        }

        "should forward processed message as invalid when key is invalid" {
            val payload = "<message><content>hello</content></message>"
            val headers = RecordHeaders()
                .add("sourceSystem", "some-system".encodeToByteArray())

            val record = mockk<FixedKeyRecord<String, String>> {
                every { key() } returns "not-a-uuid"
                every { value() } returns payload
                every { timestamp() } returns 123456789L
                every { headers() } returns headers
                every { withValue(any<ProcessedMessage>()) } answers {
                    mockk<FixedKeyRecord<String, ProcessedMessage>> {
                        every { value() } returns firstArg()
                    }
                }
            }

            val context = mockk<FixedKeyProcessorContext<String, ProcessedMessage>>(relaxed = true)

            InboundMessageProcessor(InboundMessageValidator()).apply {
                init(context)
                process(record)
            }

            val forwarded = slot<FixedKeyRecord<String, ProcessedMessage>>()

            verify(exactly = 1) {
                context.forward(capture(forwarded))
            }

            forwarded.captured.value().apply {
                this.validation.isValid() shouldBe false
                this.validation.errors().size shouldBe 1
                this.validation.errors().first().code shouldBe ErrorCode.INVALID_KAFKA_KEY
            }
        }

        "should forward processed message as invalid when value is invalid" {
            val key = Uuid.random().toString()
            val headers = RecordHeaders()
                .add("sourceSystem", "some-system".encodeToByteArray())

            val record = mockk<FixedKeyRecord<String, String>> {
                every { key() } returns key
                every { value() } returns "not valid xml"
                every { timestamp() } returns 123456789L
                every { headers() } returns headers
                every { withValue(any<ProcessedMessage>()) } answers {
                    mockk<FixedKeyRecord<String, ProcessedMessage>> {
                        every { value() } returns firstArg()
                    }
                }
            }

            val context = mockk<FixedKeyProcessorContext<String, ProcessedMessage>>(relaxed = true)

            InboundMessageProcessor(InboundMessageValidator()).apply {
                init(context)
                process(record)
            }

            val forwarded = slot<FixedKeyRecord<String, ProcessedMessage>>()

            verify(exactly = 1) {
                context.forward(capture(forwarded))
            }

            forwarded.captured.value().apply {
                this.validation.isValid() shouldBe false
                this.validation.errors().size shouldBe 1
                this.validation.errors().first().code shouldBe ErrorCode.INVALID_KAFKA_VALUE
            }
        }
    }
)
