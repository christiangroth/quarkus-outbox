package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.CoroutinesAdapter
import de.chrgroth.quarkus.outbox.domain.ExecutionAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import io.quarkus.runtime.StartupEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant

class PartitionWorkerStarterTests {

  private val coroutinesAdapter = spyk(CoroutinesAdapter())
  private val repository: OutboxRepository = mockk()
  private val executionAdapter: ExecutionAdapter = mockk()
  private val application: ApplicationPort = mockk()

  private val recovery = PartitionWorkerStarter(coroutinesAdapter, repository, executionAdapter, application)

  private val startupEvent = StartupEvent()

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  @AfterEach
  fun tearDown() {
    coroutinesAdapter.onStop()
  }

  @Test
  fun `onStart resets stale processing tasks`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns emptyList()

    recovery.onStart(startupEvent)

    verify { executionAdapter.resetStaleProcessingTasks() }
  }

  @Test
  fun `onStart activates and signals active partition`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.ACTIVE.name,
      statusReason = null,
      pausedUntil = null,
    )
    every { executionAdapter.activatePartition(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { coroutinesAdapter.wakeUp(partition) }
  }

  @Test
  fun `onStart activates and signals partition with no persisted status`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns null
    every { executionAdapter.activatePartition(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { coroutinesAdapter.wakeUp(partition) }
  }

  @Test
  fun `onStart does not reactivate manually paused partition with null pausedUntil`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "manual",
      pausedUntil = null,
    )

    recovery.onStart(startupEvent)

    verify(exactly = 0) { executionAdapter.activatePartition(any()) }
    verify(exactly = 0) { coroutinesAdapter.wakeUp(any()) }
  }

  @Test
  fun `onStart immediately reactivates paused partition whose pausedUntil has already passed`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "rate_limited",
      pausedUntil = Instant.now().minusSeconds(60),
    )
    every { executionAdapter.activatePartition(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { coroutinesAdapter.wakeUp(partition) }
  }

  @Test
  fun `onStart defers activation for paused partition whose pausedUntil is in the future`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "rate_limited",
      pausedUntil = Instant.now().plusSeconds(60),
    )

    recovery.onStart(startupEvent)

    verify(exactly = 0) { executionAdapter.activatePartition(any()) }
    verify(exactly = 0) { coroutinesAdapter.wakeUp(any()) }
  }

  @Test
  fun `onStart handles multiple partitions independently`() {
    val partitionA = object : OutboxPartition {
      override val key = "partition-a"
    }
    val partitionB = object : OutboxPartition {
      override val key = "partition-b"
    }
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partitionA, partitionB)
    every { repository.findPartition(any()) } returns null
    every { executionAdapter.activatePartition(any()) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partitionA) }
    verify { executionAdapter.activatePartition(partitionB) }
    verify { coroutinesAdapter.wakeUp(partitionA) }
    verify { coroutinesAdapter.wakeUp(partitionB) }
  }

  @Test
  fun `onStart recovers all task statuses before activating partitions`() {
    val activationOrder = mutableListOf<String>()
    every { executionAdapter.resetStaleProcessingTasks() } answers { activationOrder.add("reset") }
    every { application.getAllPartitions() } returns listOf(partition)
    every { repository.findPartition(partition) } returns null
    every { executionAdapter.activatePartition(partition) } answers { activationOrder.add("activate") }

    recovery.onStart(startupEvent)

    assertThat(activationOrder).containsExactly("reset", "activate")
  }
}
