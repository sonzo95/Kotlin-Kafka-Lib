package it.stefanosonzogni.kafka_lib

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.logstash.logback.argument.StructuredArguments
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class KafkaProducerWrapper<K, V> private constructor(
    private val topic: String,
    private val producer: Producer<K, V>
) : IKafkaProducer<K, V> {
    private var running = true
    private var healthy = true

    private val log = LoggerFactory.getLogger(KafkaProducerWrapper::class.java)

    init {
        hookShutdownHandler()
    }

    override fun close() {
        running = false
        producer.flush()
        producer.close()
        log.debug("Flushed and closed producer")
    }

    @Throws
    override suspend fun send(key: K, value: V): RecordMetadata {
        if (running && healthy) {
            val metadata: RecordMetadata?
            try {
                metadata = withContext(Dispatchers.IO) {
                    val record = ProducerRecord(topic, key, value)
                    val result = producer.send(record)
                    result.get()
                }
            } catch (e: Exception) {
                log.error("Producer threw while sending record",
                    StructuredArguments.keyValue("error", e.localizedMessage),
                    StructuredArguments.keyValue("key", key),
                    StructuredArguments.keyValue("value", value)
                )
                healthy = false
                close()
                throw KafkaProducerStateException("Producer failed sending")
            }
            return metadata!!
        }
        else {
            log.warn("Producer is not in a running state, send op will be ignored")
            throw KafkaProducerStateException("Producer is not running")
        }
    }

    override fun isReady(): Boolean { return running }
    override fun isHealthy(): Boolean { return healthy }

    private fun hookShutdownHandler() {
        val handler = Thread {
            running = false
            log.warn("Application shutting down")
            producer.flush()
            producer.close()
            log.info("Flushed and closed producer")
        }
        Runtime.getRuntime().addShutdownHook(handler)
    }

    companion object {
        fun <K, V> builder(brokers: String, clientId: String): Builder<K, V> { return Builder(brokers, clientId) }

        class Builder<K, V>(
            brokers: String,
            clientId: String
        ) {
            val properties = Properties()
            private var topic: String? = null
            private var producerImpl: Producer<K, V>? = null

            init {
                properties["bootstrap.servers"] = brokers
                properties["client.id"] = clientId
            }

            // Builder methods

            fun withTopic(topic: String): Builder<K, V> = apply { this.topic = topic }

            fun withProperty(key: String, value: String): Builder<K, V> = apply { properties[key] = value }

            fun withProperties(props: Properties): Builder<K, V> = apply {
                props.forEach { this.properties[it.key as String] = it.value as String }
            }

            fun withScramSha256Auth(username: String, password: String): Builder<K, V> = apply {
                val algorithm = "HTTPS"
                val jaasConfig = """org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""
                val protocol = "SASL_SSL"
                val mechanism = "SCRAM-SHA-256"

                properties["ssl.endpoint.identification.algorithm"] = algorithm
                properties["sasl.jaas.config"] = jaasConfig
                properties["security.protocol"] = protocol
                properties["sasl.mechanism"] = mechanism
            }

            inline fun <reified T: Serializer<K>> withKeySerializer(): Builder<K, V> = apply {
                properties["key.serializer"] = T::class.java
            }

            inline fun <reified T: Serializer<V>> withValueSerializer(): Builder<K, V> = apply {
                properties["value.serializer"] = T::class.java
            }

            fun withKeyStringSerializer(): Builder<K, V> = apply {
                properties["key.serializer"] = StringSerializer::class.java
            }

            fun withValueStringSerializer(): Builder<K, V> = apply {
                properties["value.serializer"] = StringSerializer::class.java
            }

            fun withKafkaProducerImpl(producer: Producer<K, V>) = apply { producerImpl = producer }

            fun build(): KafkaProducerWrapper<K, V> = run {
                if (topic == null) {
                    throw KafkaBuilderException("Topics undefined, use `withTopic`")
                }
                val producer = producerImpl ?: KafkaProducer(properties)
                return KafkaProducerWrapper(topic!!, producer)
            }
        }
    }
}