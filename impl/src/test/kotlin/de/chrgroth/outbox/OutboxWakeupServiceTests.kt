package de.chrgroth.outbox

import kotlinx.coroutines.channels.Channel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OutboxWakeupServiceTests {

    private val service = OutboxWakeupService()

    private val partitionA = object : OutboxPartition {
        override val key = "partition-a"
    }

    private val partitionB = object : OutboxPartition {
        override val key = "partition-b"
    }

    @Test
    fun `getOrCreate returns same channel for same partition key`() {
        val channel1 = service.getOrCreate(partitionA)
        val channel2 = service.getOrCreate(partitionA)

        assertThat(channel1).isSameAs(channel2)
    }

    @Test
    fun `getOrCreate returns different channels for different partition keys`() {
        val channelA = service.getOrCreate(partitionA)
        val channelB = service.getOrCreate(partitionB)

        assertThat(channelA).isNotSameAs(channelB)
    }

    @Test
    fun `signal sends unit to the channel`() {
        service.signal(partitionA)

        val channel = service.getOrCreate(partitionA)
        val result = channel.tryReceive()
        assertThat(result.isSuccess).isTrue()
    }
}
