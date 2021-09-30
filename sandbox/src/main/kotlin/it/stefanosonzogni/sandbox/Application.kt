package it.stefanosonzogni.sandbox

import it.stefanosonzogni.kafka_lib.health.KafkaHealthClient
import kotlinx.coroutines.runBlocking

class Application {

    companion object {
        @JvmStatic
        fun main(args : Array<String>) {
            val healthClient = KafkaHealthClient.builder("localhost:9092").build()
            runBlocking {
                print(healthClient.describeCluster().getOrNull())
//                print(healthClient.describeTopics(listOf("")))
            }
        }
    }
}