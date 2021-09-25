package it.stefanosonzogni.kafka_lib.consumer.processingstrategies

import io.mockk.*
import it.stefanosonzogni.kafka_lib.IKafkaProcessor
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LocalRetryProcessingStrategyTest {

    private val processor = mockk<IKafkaProcessor<String, String>>()

    @BeforeEach
    fun setup() {
        clearAllMocks()
    }

    @Test
    fun `Processing is called once if it doesn't throw`() {
        val record = ConsumerRecord("key", "value")
        coEvery { processor.process(record) }
        val strategy = LocalRetryProcessingStrategy<String, String>()
        runBlocking { strategy.handleRecord(record, processor) }
        coVerifySequence { processor.process(record) }
    }

    @Test
    fun `Processing is called n times if it always fails`() {
        val record = ConsumerRecord("key", "value")
        coEvery { processor.process(record) }.throws(Exception("some error"))
        val strategy = LocalRetryProcessingStrategy<String, String>(retriesLimit = 3, retriesDelayMs = 50)
        runBlocking { strategy.handleRecord(record, processor) }
        coVerify(exactly = 4) { processor.process(record) }
    }

    @Test
    fun `Processing is called x+1 times if it fails x times`() {
        val record = ConsumerRecord("key", "value")
        coEvery { processor.process(record) }.throws(Exception("some error")).andThen(Unit)
        val strategy = LocalRetryProcessingStrategy<String, String>(retriesLimit = 3, retriesDelayMs = 50)
        runBlocking { strategy.handleRecord(record, processor) }
        coVerify(exactly = 2) { processor.process(record) }
    }

    private fun ConsumerRecord(key: String, value: String): ConsumerRecord<String, String> =
        ConsumerRecord("topic", 1, 100, key, value)

}