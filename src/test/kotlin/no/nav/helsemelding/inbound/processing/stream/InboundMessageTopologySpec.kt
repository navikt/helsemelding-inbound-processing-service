package no.nav.helsemelding.inbound.processing.stream

import arrow.core.left
import arrow.core.right
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import no.nav.helsemelding.inbound.processing.config
import no.nav.helsemelding.messageconverter.MessageConverter
import no.nav.helsemelding.messageconverter.error.MappingError
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
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
            val convertedJson = """{"converted": true}"""
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

                inputTopic.pipeInput(TestRecord(validKey, validXml, RecordHeaders()))

                outboundTopic.readValue() shouldBe convertedJson
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

                inputTopic.pipeInput(TestRecord("not-a-uuid", "not valid xml", RecordHeaders()))

                outboundTopic.isEmpty shouldBe true
            }
        }

        "should discard message that fails on conversion" {
            val messageConverter = mockk<MessageConverter>()
            every { messageConverter.incomingDialogMessageXmlToJson(any()) } returns
                MappingError("Unsupported message type").left()

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

                inputTopic.pipeInput(TestRecord(validKey, validXml, RecordHeaders()))

                outboundTopic.isEmpty shouldBe true
            }
        }
    }
)
