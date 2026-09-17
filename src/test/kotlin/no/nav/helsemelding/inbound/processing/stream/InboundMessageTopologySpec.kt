package no.nav.helsemelding.inbound.processing.stream

import arrow.core.left
import arrow.core.right
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import no.nav.helsemelding.inbound.processing.config
import no.nav.helsemelding.inbound.processing.stream.exception.FatalConversionException
import no.nav.helsemelding.messageconverter.MessageConverter
import no.nav.helsemelding.messageconverter.error.MappingError
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.test.TestRecord

class InboundMessageTopologySpec : StringSpec(
    {
        val kafkaStreams = config().kafkaStreamsSettings
        val validXml = "<message><content>hello</content></message>"
        val validKey = java.util.UUID.randomUUID().toString()

        fun buildDriver(messageConverter: MessageConverter) = TopologyTestDriver(
            InboundMessageTopology(InboundMessageValidator(), messageConverter).build(),
            kafkaStreams.toProperties()
        )

        "should route valid JSON message to outbound topic" {
            val convertedJson = """{"converted": true, "numberOfAttachments": 0}"""
            val messageConverter = mockk<MessageConverter>()
            every { messageConverter.incomingDialogMessageXmlToJson(any()) } returns convertedJson.right()

            buildDriver(messageConverter).use { driver ->
                val inputTopic = driver.createInputTopic(
                    kafkaStreams.topics.dialogMessageIn,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
                )
                val outboundTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageOut,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                inputTopic.pipeInput(
                    TestRecord(
                        validKey,
                        validXml,
                        RecordHeaders().add(ATTACHMENT_COUNT_HEADER, "2".encodeToByteArray())
                    )
                )

                outboundTopic.readValue() shouldBe """{"converted":true,"numberOfAttachments":2}"""
            }
        }

        "should discard message that fails on validation" {
            val messageConverter = mockk<MessageConverter>()

            buildDriver(messageConverter).use { driver ->
                val inputTopic = driver.createInputTopic(
                    kafkaStreams.topics.dialogMessageIn,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
                )
                val outboundTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageOut,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                inputTopic.pipeInput(
                    TestRecord(
                        "not-a-uuid",
                        "not valid xml",
                        RecordHeaders().add(ATTACHMENT_COUNT_HEADER, "2".encodeToByteArray())
                    )
                )

                outboundTopic.isEmpty shouldBe true
            }
        }

        "should throw exception when conversion fails" {
            val messageConverter = mockk<MessageConverter>()
            every { messageConverter.incomingDialogMessageXmlToJson(any()) } returns
                MappingError("Unsupported message type").left()

            val exception = shouldThrow<StreamsException> {
                buildDriver(messageConverter).use { driver ->
                    val inputTopic = driver.createInputTopic(
                        kafkaStreams.topics.dialogMessageIn,
                        Serdes.String().serializer(),
                        Serdes.String().serializer()
                    )

                    inputTopic.pipeInput(
                        TestRecord(
                            validKey,
                            validXml,
                            RecordHeaders().add(ATTACHMENT_COUNT_HEADER, "2".encodeToByteArray())
                        )
                    )
                }
            }

            exception.message shouldBe "Exception caught in process. taskId=0_0, processor=KSTREAM-FLATMAPVALUES-0000000005, topic=helsemelding.dialog.in.xml, partition=0, offset=0"
            exception.cause shouldBe FatalConversionException("Failed to convert XML to JSON: Unsupported message type")
        }

        "should discard converted JSON without numberOfAttachments" {
            val messageConverter = mockk<MessageConverter>()
            every { messageConverter.incomingDialogMessageXmlToJson(any()) } returns """{"converted": true}""".right()

            buildDriver(messageConverter).use { driver ->
                val inputTopic = driver.createInputTopic(
                    kafkaStreams.topics.dialogMessageIn,
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
                )
                val outboundTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageOut,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                inputTopic.pipeInput(
                    TestRecord(
                        validKey,
                        validXml,
                        RecordHeaders().add(ATTACHMENT_COUNT_HEADER, "2".encodeToByteArray())
                    )
                )

                outboundTopic.isEmpty shouldBe true
            }
        }
    }
)
