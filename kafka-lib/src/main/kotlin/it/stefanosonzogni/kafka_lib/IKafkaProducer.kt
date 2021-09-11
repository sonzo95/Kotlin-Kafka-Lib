package it.stefanosonzogni.kafka_lib

import it.stefanosonzogni.service_lib.health.Diagnosable
import org.apache.kafka.clients.producer.RecordMetadata

interface IKafkaProducer<K, V> : Diagnosable {
    fun close()
    suspend fun send(key: K, value: V): RecordMetadata
}