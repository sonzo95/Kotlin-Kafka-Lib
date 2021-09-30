package it.stefanosonzogni.kafka_lib.health

import org.apache.kafka.common.Node

data class DescribeClusterResponse(
    val nodes: Collection<Node>,
    val clusterId: String
)
