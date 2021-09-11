package it.stefanosonzogni.kafka_lib

import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.concurrent.locks.ReentrantLock


class KafkaConsumerWrapper<K, V> private constructor(
    private val topics: List<String>,
    private val processor: IKafkaProcessor<K, V>,
    private val config: Config,
    private val consumer: Consumer<K, V>,
    private val clock: Clock = Clock.systemDefaultZone()
) : IKafkaConsumer {
    private var running = false
    private var healthy = false

    private val log = LoggerFactory.getLogger(KafkaConsumerWrapper::class.java)
    private val workerThread = Thread { listen() }
    // guarantees that the consumer finishes processing its current record before shutdown
    private val runningLock = ReentrantLock()

    override fun start() {
        consumer.subscribe(topics)
        workerThread.start()
        hookShutdownHandler()
    }

    private fun listen() {
        log.info("Kafka consumer connected, starting polling",
            StructuredArguments.keyValue("topics", topics)
        )
        running = true
        runningLock.lock()
        var lastPollEpoch: Long

        while (running) {
            val records = try {
                val records = consumer.poll(Duration.ofMillis(100))
                healthy = true
                lastPollEpoch = clock.instant().toEpochMilli()
                records
            } catch (e: Exception) {
                healthy = false
                running = false
                log.error("FATAL: Kafka consumer broke",
                    StructuredArguments.keyValue("topics", topics),
                    StructuredArguments.keyValue("error", e.localizedMessage)
                )
                break
            }

            var lastProcessedTopic: String? = null
            var lastProcessedPartition: Int? = null
            var lastProcessedOffset: Long? = null
            records?.let {
                for (record in records) {
                    if (!running) {
                        break
                    }
                    if(record != null)
                        handleRecord(record)
                    lastProcessedTopic = record.topic()
                    lastProcessedPartition = record.partition()
                    lastProcessedOffset = record.offset()
                }
            }
            if (lastProcessedTopic != null && lastProcessedPartition != null && lastProcessedOffset != null) {
                try {
                    val topicAndPartition = TopicPartition(lastProcessedTopic, lastProcessedPartition!!)
                    val offset = OffsetAndMetadata(lastProcessedOffset!! + 1)
                    consumer.commitSync(mapOf(topicAndPartition to offset))
                } catch (e: Exception) {
                    log.error("Kafka consumer tried to commit an offset but failed",
                        StructuredArguments.keyValue("topic", lastProcessedTopic),
                        StructuredArguments.keyValue("partition", lastProcessedPartition),
                        StructuredArguments.keyValue("offset", lastProcessedOffset)
                    )
                }
            }

            val elapsedTimeFromLastPollMs = clock.instant().toEpochMilli() - lastPollEpoch
            if (elapsedTimeFromLastPollMs < config.minIntervalBetweenPollsMs) {
                Thread.sleep(config.minIntervalBetweenPollsMs - elapsedTimeFromLastPollMs)
            }
        }
        log.info("Kafka consumer disconnecting...",
            StructuredArguments.keyValue("topics", topics)
        )
        consumer.unsubscribe()
        consumer.close()
        log.info("Kafka consumer terminated",
            StructuredArguments.keyValue("topics", topics)
        )
        runningLock.unlock()
    }

    private fun handleRecord(record: ConsumerRecord<K, V>) {
        var attemptsLeft = config.processingRetriesLimit + 1
        var success = false
        while (attemptsLeft > 0 && !success) {
            try {
                runBlocking { processor.process(record) }
                success = true
            } catch (e: Exception) {
                log.debug("Record processing ended with error",
                    StructuredArguments.keyValue("topic", record.topic()),
                    StructuredArguments.keyValue("record key", record.key()),
                    StructuredArguments.keyValue("error", e.localizedMessage)
                )
                attemptsLeft--
                Thread.sleep(config.processingRetriesDelayMs)
            }
        }
        if (!success) {
            log.error("Record processing failed",
                StructuredArguments.keyValue("topic", record.topic()),
                StructuredArguments.keyValue("record key", record.key()),
                StructuredArguments.keyValue("record value", record.value())
            )
        }
    }

    override fun stop() {
        running = false
    }

    override fun isReady(): Boolean {
        return running
    }

    override fun isHealthy(): Boolean {
        return healthy
    }

    private fun hookShutdownHandler() {
        val handler = Thread {
            running = false
            log.warn("Application shutting down")
            log.info("Waiting for consumer to finish its current job")
            runningLock.lock()
            log.info("Handlers finished, shutting down...")
        }
        Runtime.getRuntime().addShutdownHook(handler)
    }


    // Config

    data class Config(
        val processingRetriesLimit: Int = 0,
        val processingRetriesDelayMs: Long = 100,
        val minIntervalBetweenPollsMs: Long = 0
    )


    companion object {
        fun <K, V> builder(brokers: String, groupId: String): Builder<K, V> { return Builder(brokers, groupId) }

        class Builder<K, V>(
            brokers: String,
            groupId: String
        ) {
            val properties = Properties()
            private var topics: List<String>? = null
            private var processor: IKafkaProcessor<K, V>? = null
            private var config: Config = Config()
            private var consumerImpl: Consumer<K, V>? = null

            init {
                properties["bootstrap.servers"] = brokers
                properties["group.id"] = groupId
                properties["enable.auto.commit"] = false
            }

            // Builder methods

            fun withTopic(topic: String): Builder<K, V> = apply { topics = listOf(topic) }
            fun withTopics(topics: List<String>): Builder<K, V> = apply { this.topics = topics }

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

            inline fun <reified T: Deserializer<K> > withKeyDeserializer(): Builder<K, V> = apply {
                properties["key.deserializer"] = T::class.java
            }

            inline fun <reified T: Deserializer<V> > withValueDeserializer(): Builder<K, V> = apply {
                properties["value.deserializer"] = T::class.java
            }

            fun withKeyStringDeserializer(): Builder<K, V> = apply {
                properties["key.deserializer"] = StringDeserializer::class.java
            }

            fun withValueStringDeserializer(): Builder<K, V> = apply {
                properties["value.deserializer"] = StringDeserializer::class.java
            }

            fun withProcessor(processor: IKafkaProcessor<K, V>) = apply {
                this.processor = processor
            }

            fun withConfig(config: Config) = apply { this.config = config }

            fun withKafkaConsumerImpl(consumer: Consumer<K, V>) = apply { consumerImpl = consumer }

            fun build(): KafkaConsumerWrapper<K, V> = run {
                if (topics == null) {
                    throw KafkaBuilderException("Topics undefined, use `withTopic` or `withTopics`")
                }
                if (processor == null) {
                    throw KafkaBuilderException("No processor given, use `withProcessor`")
                }
                val consumer = consumerImpl ?: KafkaConsumer(properties)
                return KafkaConsumerWrapper(topics!!, processor!!, config, consumer)
            }
        }
    }
}