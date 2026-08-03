package no.nav.helsemelding.inbound.processing.stream

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import no.nav.helsemelding.inbound.processing.config
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord

class InboundMessageTopologySpec : StringSpec(
    {
        val kafkaStreams = config().kafkaStreamsSettings

        "should route invalid message to error topic" {
            val testDriver = TopologyTestDriver(
                InboundMessageTopology(InboundMessageValidator()).build(),
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

                val errorTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageError,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                val key = "not-a-uuid"
                val payload = "not valid xml"
                val headers = RecordHeaders()

                inputTopic.pipeInput(
                    TestRecord(
                        key,
                        payload,
                        headers
                    )
                )

                outboundTopic.isEmpty shouldBe true

                val errors = errorTopic.readRecordsToList()

                errors.size shouldBe 1

                val errorRecord = errors.single()

                errorRecord.key() shouldBe key

                errorRecord.value().also { json ->
                    json shouldContain "INVALID_KAFKA_KEY"
                    json shouldContain "INVALID_KAFKA_VALUE"
                    json shouldContain "MISSING_SOURCE_SYSTEM_HEADER"
                }
            }
        }

        "should route valid message to outbound topic" {
            val testDriver = TopologyTestDriver(
                InboundMessageTopology(InboundMessageValidator()).build(),
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

                val errorTopic = driver.createOutputTopic(
                    kafkaStreams.topics.dialogMessageError,
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )

                val key = java.util.UUID.randomUUID().toString()
                val payload = "<message><content>hello</content></message>"
                val headers = RecordHeaders()
                    .add("sourceSystem", "some-system".encodeToByteArray())

                inputTopic.pipeInput(TestRecord(key, payload, headers))

                errorTopic.isEmpty shouldBe true
                outboundTopic.isEmpty shouldBe false
            }
        }
    }
)
