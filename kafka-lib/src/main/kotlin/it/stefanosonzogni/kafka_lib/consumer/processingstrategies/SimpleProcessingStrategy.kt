package it.stefanosonzogni.kafka_lib.consumer.processingstrategies

import it.stefanosonzogni.kafka_lib.IKafkaProcessor
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

/**
 * Strategy that tries to process a record while catching any exceptions. If any is caught, the record is considered
 * processed and its offset is committed.
 */
class SimpleProcessingStrategy<K, V> : RecordProcessingStrategy<K, V> {
    private val log = LoggerFactory.getLogger(SimpleProcessingStrategy::class.java)

    override fun handleRecord(record: ConsumerRecord<K, V>, processor: IKafkaProcessor<K, V>) {
        try {
            runBlocking { processor.process(record) }
        } catch (e: Exception) {
            log.error("Record processing failed",
                StructuredArguments.keyValue("topic", record.topic()),
                StructuredArguments.keyValue("record key", record.key()),
                StructuredArguments.keyValue("record value", record.value())
            )
        }
    }
}