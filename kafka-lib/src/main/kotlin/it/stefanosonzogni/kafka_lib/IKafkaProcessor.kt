package it.stefanosonzogni.kafka_lib

import org.apache.kafka.clients.consumer.ConsumerRecord

interface IKafkaProcessor<K, V> {
    suspend fun process(record: ConsumerRecord<K, V>)
}