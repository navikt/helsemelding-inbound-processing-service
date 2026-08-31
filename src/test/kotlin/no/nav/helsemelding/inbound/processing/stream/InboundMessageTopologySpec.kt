package no.nav.helsemelding.inbound.processing.stream

import arrow.core.right
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import no.nav.helsemelding.inbound.processing.config
import no.nav.helsemelding.messageconverter.MessageConverter
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord

class InboundMessageTopologySpec : StringSpec(
    {
        val kafkaStreams = config().kafkaStreamsSettings

        "should route valid message to outbound topic" {
            val convertedJson = """{"converted": true}"""
            val messageConverter = mockk<MessageConverter>()
            every { messageConverter.incomingDialogMessageXmlToJson(any()) } returns convertedJson.right()

            val testDriver = TopologyTestDriver(
                InboundMessageTopology(InboundMessageValidator(), messageConverter).build(),
                kafkaStreams.toProperties()
            )

            testDriver.use { driver ->
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

                val key = java.util.UUID.randomUUID().toString()
                val payload = "<message><content>hello</content></message>"
                val headers = RecordHeaders()
                    .add("sourceSystem", "some-system".encodeToByteArray())

                inputTopic.pipeInput(TestRecord(key, payload, headers))

                outboundTopic.isEmpty shouldBe false
            }
        }
    }
)
