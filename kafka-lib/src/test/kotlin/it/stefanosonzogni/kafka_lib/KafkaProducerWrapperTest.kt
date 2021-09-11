package it.stefanosonzogni.kafka_lib

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.util.*
import java.util.concurrent.CompletableFuture

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaProducerWrapperTest {

    private val topic = "test"
    private val clientId = "test-client"
    private val brokers = "localhost:9092"
    private val producerMock = mockk<MockProducer<String, String>>(relaxUnitFun = true)

    @BeforeEach
    fun setup() {
        clearAllMocks()
    }

    @Test
    fun `Producer builder properties work correctly`() {
        val builder = KafkaProducerWrapper.builder<String, String>(brokers, clientId)
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["client.id"] = clientId
        })

        builder.withScramSha256Auth("user", "pass")
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["client.id"] = clientId
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "SASL_SSL"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
        })

        builder.withProperty("security.protocol", "asd")
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["client.id"] = clientId
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "asd"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
        })

        builder.withProperties(Properties().apply { this["client.id"] = "client" })
        assertThat(builder.properties).isEqualTo(Properties().apply {
            this["bootstrap.servers"] = brokers
            this["client.id"] = clientId
            this["ssl.endpoint.identification.algorithm"] = "HTTPS"
            this["sasl.jaas.config"] = """org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="pass";"""
            this["security.protocol"] = "asd"
            this["sasl.mechanism"] = "SCRAM-SHA-256"
            this["client.id"] = "client"
        })
    }

    @Test
    fun `Producer sends records and shuts down`() {
        val defaultRecordMetadata = RecordMetadata(TopicPartition(topic, 0), 0, 0, 0, 0, 0, 0)

        every { producerMock.send(ProducerRecord(topic, "key", "value")) }.returns(
            CompletableFuture.completedFuture(defaultRecordMetadata)
        )

        val producer = KafkaProducerWrapper.builder<String, String>(brokers, clientId)
            .withKeyStringSerializer()
            .withValueStringSerializer()
            .withTopic(topic)
            .withKafkaProducerImpl(producerMock)
            .build()

        assertThat(producer.isHealthy()).isTrue()
        assertThat(producer.isReady()).isTrue()

        val metadata = runBlocking { producer.send("key", "value") }

        assertThat(metadata).isEqualTo(defaultRecordMetadata)

        producer.close()

        assertThat(producer.isHealthy()).isTrue()
        assertThat(producer.isReady()).isFalse()
        assertThrows<KafkaProducerStateException> { runBlocking { producer.send("key", "value") } }
    }

    @Test
    fun `Producer shuts down and is unhealthy if send throws`() {
        every { producerMock.send(ProducerRecord(topic, "key", "value")) }.returns(
            CompletableFuture.failedFuture(KafkaException("Failure"))
        )

        val producer = KafkaProducerWrapper.builder<String, String>(brokers, clientId)
            .withKeyStringSerializer()
            .withValueStringSerializer()
            .withTopic(topic)
            .withKafkaProducerImpl(producerMock)
            .build()

        assertThrows<KafkaProducerStateException> { runBlocking { producer.send("key", "value") } }

        assertThat(producer.isHealthy()).isFalse()
        assertThat(producer.isReady()).isFalse()
    }

}