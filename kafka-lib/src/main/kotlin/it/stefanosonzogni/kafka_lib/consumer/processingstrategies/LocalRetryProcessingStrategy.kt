package it.stefanosonzogni.kafka_lib.consumer.processingstrategies

import it.stefanosonzogni.kafka_lib.IKafkaProcessor
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

/**
 * Strategy that tries to process a record while catching any exceptions and, if any is found, retries processing
 * after waiting a fixed amount of milliseconds. If the retries limit is reached, the record is considered processed
 * and its offset is committed.
 *
 * @param retriesLimit: Number of retries allowed. 0 means no retries.
 * @param retriesDelayMs: Amount of time in milliseconds that are waited after an exception is caught.
 */
class LocalRetryProcessingStrategy<K, V>(
    private val retriesLimit: Int = 0,
    private val retriesDelayMs: Long = 100,
) : RecordProcessingStrategy<K, V> {
    private val log = LoggerFactory.getLogger(LocalRetryProcessingStrategy::class.java)

    override fun handleRecord(record: ConsumerRecord<K, V>, processor: IKafkaProcessor<K, V>) {
        var attemptsLeft = retriesLimit + 1
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
                Thread.sleep(retriesDelayMs)
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
}