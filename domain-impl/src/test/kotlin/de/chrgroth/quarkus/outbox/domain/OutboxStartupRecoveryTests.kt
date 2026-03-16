package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.OutboxTaskDispatcher
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.quarkus.runtime.StartupEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant

class OutboxStartupRecoveryTests {

    private val outbox: OutboxImpl = mockk()
    private val dispatcher: OutboxTaskDispatcher = mockk()

    private val recovery = OutboxStartupRecovery(outbox, dispatcher)

    private val startupEvent = StartupEvent()

    private val partition = object : OutboxPartition {
        override val key = "test-partition"
    }

    @AfterEach
    fun tearDown() {
        recovery.onStop()
    }

    @Test
    fun `onStart resets stale processing tasks`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns emptyList()

        recovery.onStart(startupEvent)

        verify { outbox.resetStaleProcessingTasks() }
    }

    @Test
    fun `onStart activates and signals active partition`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns OutboxPartitionInfo(
            key = partition.key,
            status = OutboxPartitionStatus.ACTIVE.name,
            statusReason = null,
            pausedUntil = null,
        )
        every { outbox.activatePartition(partition) } just runs
        every { outbox.signal(partition) } just runs

        recovery.onStart(startupEvent)

        verify { outbox.activatePartition(partition) }
        verify { outbox.signal(partition) }
    }

    @Test
    fun `onStart activates and signals partition with no persisted status`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns null
        every { outbox.activatePartition(partition) } just runs
        every { outbox.signal(partition) } just runs

        recovery.onStart(startupEvent)

        verify { outbox.activatePartition(partition) }
        verify { outbox.signal(partition) }
    }

    @Test
    fun `onStart immediately reactivates paused partition with null pausedUntil`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns OutboxPartitionInfo(
            key = partition.key,
            status = OutboxPartitionStatus.PAUSED.name,
            statusReason = "rate_limited",
            pausedUntil = null,
        )
        every { outbox.activatePartition(partition) } just runs
        every { outbox.signal(partition) } just runs

        recovery.onStart(startupEvent)

        verify { outbox.activatePartition(partition) }
        verify { outbox.signal(partition) }
    }

    @Test
    fun `onStart immediately reactivates paused partition whose pausedUntil has already passed`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns OutboxPartitionInfo(
            key = partition.key,
            status = OutboxPartitionStatus.PAUSED.name,
            statusReason = "rate_limited",
            pausedUntil = Instant.now().minusSeconds(60),
        )
        every { outbox.activatePartition(partition) } just runs
        every { outbox.signal(partition) } just runs

        recovery.onStart(startupEvent)

        verify { outbox.activatePartition(partition) }
        verify { outbox.signal(partition) }
    }

    @Test
    fun `onStart defers activation for paused partition whose pausedUntil is in the future`() {
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns OutboxPartitionInfo(
            key = partition.key,
            status = OutboxPartitionStatus.PAUSED.name,
            statusReason = "rate_limited",
            pausedUntil = Instant.now().plusSeconds(60),
        )

        recovery.onStart(startupEvent)

        verify(exactly = 0) { outbox.activatePartition(any()) }
        verify(exactly = 0) { outbox.signal(any()) }
    }

    @Test
    fun `onStart handles multiple partitions independently`() {
        val partitionA = object : OutboxPartition {
            override val key = "partition-a"
        }
        val partitionB = object : OutboxPartition {
            override val key = "partition-b"
        }
        every { outbox.resetStaleProcessingTasks() } just runs
        every { dispatcher.partitions } returns listOf(partitionA, partitionB)
        every { outbox.findPartition(any()) } returns null
        every { outbox.activatePartition(any()) } just runs
        every { outbox.signal(any()) } just runs

        recovery.onStart(startupEvent)

        verify { outbox.activatePartition(partitionA) }
        verify { outbox.activatePartition(partitionB) }
        verify { outbox.signal(partitionA) }
        verify { outbox.signal(partitionB) }
    }

    @Test
    fun `onStart recovers all task statuses before activating partitions`() {
        val activationOrder = mutableListOf<String>()
        every { outbox.resetStaleProcessingTasks() } answers { activationOrder.add("reset") }
        every { dispatcher.partitions } returns listOf(partition)
        every { outbox.findPartition(partition) } returns null
        every { outbox.activatePartition(partition) } answers { activationOrder.add("activate") }
        every { outbox.signal(partition) } just runs

        recovery.onStart(startupEvent)

        assertThat(activationOrder).containsExactly("reset", "activate")
    }
}
