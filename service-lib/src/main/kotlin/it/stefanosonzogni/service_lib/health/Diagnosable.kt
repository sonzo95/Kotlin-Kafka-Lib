package it.stefanosonzogni.service_lib.health

interface Diagnosable {
    fun isHealthy(): Boolean
    fun isReady(): Boolean
}