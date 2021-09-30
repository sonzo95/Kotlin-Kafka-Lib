package it.stefanosonzogni.kafka_lib.health

import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.TopicDescription
import java.util.*

class KafkaHealthClient private constructor(
    props: Properties
) {
    private val adminClient = AdminClient.create(props)

    suspend fun describeCluster(): Result<DescribeClusterResponse> {
        val description = adminClient.describeCluster()
        return withContext(Dispatchers.IO) {
            try {
                val clusterId = description.clusterId().get()
                val nodes = description.nodes().get()

                return@withContext if (nodes.isNotEmpty())
                    Result.success(
                        DescribeClusterResponse(
                            nodes, clusterId
                        )
                    )
                else
                    Result.failure(Exception("No connected nodes were found"))
            } catch (e: Exception) {
                Result.failure(e)
            }
        }
    }

    suspend fun describeTopics(topics: Collection<String>): Result<Map<String, TopicDescription>> {
        val description = adminClient.describeTopics(topics)
        return withContext(Dispatchers.IO) {
            try {
                val topicMap = description.all().get()
                return@withContext if (topicMap.size == topics.size)
                    Result.success(topicMap)
                else
                    Result.failure(Exception("Some descriptions were not found for given topics"))
            } catch (e: Exception) {
                Result.failure(e)
            }
        }
    }

    companion object {
        fun builder(brokers: String, timeoutMs: Int = 5_000): Builder { return Builder(brokers, timeoutMs) }

        class Builder(
            brokers: String,
            timeoutMs: Int
        ) {
            private val properties = Properties()

            init {
                properties["bootstrap.servers"] = brokers
                properties["request.timeout.ms"] = timeoutMs
            }

            fun withProperty(key: String, value: String): Builder = apply { properties[key] = value }

            fun withProperties(props: Properties): Builder = apply {
                props.forEach { this.properties[it.key as String] = it.value as String }
            }

            fun withScramSha256Auth(username: String, password: String): Builder = apply {
                val algorithm = "HTTPS"
                val jaasConfig = """org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""
                val protocol = "SASL_SSL"
                val mechanism = "SCRAM-SHA-256"

                properties["ssl.endpoint.identification.algorithm"] = algorithm
                properties["sasl.jaas.config"] = jaasConfig
                properties["security.protocol"] = protocol
                properties["sasl.mechanism"] = mechanism
            }

            fun build(): KafkaHealthClient = KafkaHealthClient(properties)
        }
    }
}