package it.stefanosonzogni.kafka_lib

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import io.mockk.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.lang.Integer.min
import java.time.Duration
import java.util.*
import java.util.stream.Stream

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerWrapperTest {

    private val topic = "test"
    private val groupId = "test-group"
    private val brokers = "localhost:9092"
    private val consumerMock = mockk<MockConsumer<String, String>>(relaxUnitFun = true)

    @BeforeEach
    fun setup() {
        clearAllMocks()
    }

    @Test
    fun `Consumer builder properties work correctly`() {
        val builder = KafkaConsumerWrapper.builder<String, String>(brokers, groupId)
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["group.id"] = groupId
            this["enable.auto.commit"] = false
        })

        builder.withScramSha256Auth("user", "pass")
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["group.id"] = groupId
            this["enable.auto.commit"] = false
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "SASL_SSL"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
        })

        builder.withProperty("security.protocol", "asd")
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["group.id"] = groupId
            this["enable.auto.commit"] = false
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "asd"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
        })

        builder.withProperties(Properties().apply { this["client.id"] = "client" })
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["group.id"] = groupId
            this["enable.auto.commit"] = false
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "asd"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
            this["client.id"] = "client"
        })
    }

    @Test
    fun `Consumer processes records and shuts down`() {
        val partition = 0
        val firstSampleRecord = ConsumerRecord(topic, partition, 100L, "key", "value")
        val secondSampleRecord = ConsumerRecord(topic, partition, 101L, "key", "value")
        val processor = mockk<IKafkaProcessor<String, String>>()
        coEvery { processor.process(firstSampleRecord) }.returns(Unit)

        every { consumerMock.poll(any<Duration>()) }.returns(
            ConsumerRecords(
                mapOf(
                    TopicPartition(topic, partition) to listOf(
                        firstSampleRecord,
                        secondSampleRecord
                    )
                )
            )
        ).andThen(ConsumerRecords(
            mapOf(
                TopicPartition(topic, partition) to listOf()
            )
        ))

        val consumer = KafkaConsumerWrapper.builder<String, String>(brokers, groupId)
            .withKeyStringDeserializer()
            .withValueStringDeserializer()
            .withTopic(topic)
            .withProcessor(processor)
            .withKafkaConsumerImpl(consumerMock)
            .build()

        assertThat(consumer.isHealthy()).isFalse()
        assertThat(consumer.isReady()).isFalse()

        consumer.start()

        Thread.sleep(200)

        verify(exactly = 1) { consumerMock.subscribe(listOf(topic)) }
        assertThat(consumer.isHealthy()).isTrue()
        assertThat(consumer.isReady()).isTrue()
        verify(atLeast = 1) { consumerMock.poll(any<Duration>()) }
        verify(exactly = 1) { consumerMock.commitSync(
            mapOf(TopicPartition(topic, partition) to OffsetAndMetadata(102L))
        ) }
        coVerify(exactly = 1) { processor.process(firstSampleRecord) }
        coVerify(exactly = 1) { processor.process(secondSampleRecord) }

        consumer.stop()

        Thread.sleep(100)

        verify(exactly = 1) { consumerMock.unsubscribe() }
        verify(exactly = 1) { consumerMock.close() }
        assertThat(consumer.isReady()).isFalse()
    }

    @Test
    fun `Consumer processes records with given min poll interval`() {
        val partition = 0
        val processor = mockk<IKafkaProcessor<String, String>>()

        every { consumerMock.poll(any<Duration>()) }.returns(
            ConsumerRecords(
                mapOf(
                    TopicPartition(topic, partition) to listOf()
                )
            )
        )

        val config = KafkaConsumerWrapper.Config(minIntervalBetweenPollsMs = 200)
        val consumer = KafkaConsumerWrapper.builder<String, String>(brokers, groupId)
            .withKeyStringDeserializer()
            .withValueStringDeserializer()
            .withTopic(topic)
            .withProcessor(processor)
            .withKafkaConsumerImpl(consumerMock)
            .withConfig(config)
            .build()

        assertThat(consumer.isHealthy()).isFalse()
        assertThat(consumer.isReady()).isFalse()

        consumer.start()

        Thread.sleep(100)

        verify(exactly = 1) { consumerMock.subscribe(listOf(topic)) }
        assertThat(consumer.isHealthy()).isTrue()
        assertThat(consumer.isReady()).isTrue()
        verify(exactly = 1) { consumerMock.poll(any<Duration>()) }
        Thread.sleep(200)
        verify(exactly = 2) { consumerMock.poll(any<Duration>()) }
        Thread.sleep(200)
        verify(exactly = 3) { consumerMock.poll(any<Duration>()) }

        consumer.stop()
        Thread.sleep(100)

        verify(exactly = 1) { consumerMock.unsubscribe() }
        verify(exactly = 1) { consumerMock.close() }
        assertThat(consumer.isReady()).isFalse()
    }

    @Test
    fun `Consumer shuts down and is unhealthy if poll throws`() {
        val processor = mockk<IKafkaProcessor<String, String>>()

        every { consumerMock.poll(any<Duration>()) }.throws(KafkaException("some exception"))

        val consumer = KafkaConsumerWrapper.builder<String, String>(brokers, groupId)
            .withKeyStringDeserializer()
            .withValueStringDeserializer()
            .withTopic(topic)
            .withProcessor(processor)
            .withKafkaConsumerImpl(consumerMock)
            .build()

        assertThat(consumer.isHealthy()).isFalse()
        assertThat(consumer.isReady()).isFalse()

        consumer.start()

        Thread.sleep(200)

        verify(exactly = 1) { consumerMock.subscribe(listOf(topic)) }
        verify(exactly = 1) { consumerMock.unsubscribe() }
        verify(exactly = 1) { consumerMock.close() }
        assertThat(consumer.isHealthy()).isFalse()
        assertThat(consumer.isReady()).isFalse()
        verify(exactly = 1) { consumerMock.poll(any<Duration>()) }
        verify(exactly = 0) { consumerMock.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

}