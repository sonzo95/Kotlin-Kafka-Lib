package it.stefanosonzogni.kafka_lib.consumer.processingstrategies

import it.stefanosonzogni.kafka_lib.IKafkaProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord

interface RecordProcessingStrategy<K, V> {
    fun handleRecord(record: ConsumerRecord<K, V>, processor: IKafkaProcessor<K, V>)
}