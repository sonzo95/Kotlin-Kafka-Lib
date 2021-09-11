package it.stefanosonzogni.kafka_lib

import it.stefanosonzogni.service_lib.health.Diagnosable

interface IKafkaConsumer : Diagnosable {
    fun start()
    fun stop()
}