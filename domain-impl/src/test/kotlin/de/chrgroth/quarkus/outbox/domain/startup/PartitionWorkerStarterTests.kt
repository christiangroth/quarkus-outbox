package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.OutboxControllerAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant

class PartitionWorkerStarterTests {

  private val testScope = CoroutineScope(Dispatchers.IO)
  private val coroutinesPort: CoroutinesPort = mockk(relaxUnitFun = true) {
    every { getScope() } returns testScope
  }
  private val partitionPort: PartitionRepositoryPort = mockk()
  private val executionAdapter: OutboxControllerAdapter = mockk()
  private val application: ApplicationPort = mockk()

  private val recovery = PartitionWorkerStarter(coroutinesPort, partitionPort, executionAdapter, application)

  private val startupEvent = StartupEvent()

  private val partition = object : ApplicationOutboxPartition {
    override val key = "test-partition"
  }

  @AfterEach
  fun tearDown() {
    testScope.cancel()
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
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.ACTIVE,
      statusReason = null,
      pausedUntil = null,
    )
    every { executionAdapter.activatePartition(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { coroutinesPort.signal(partition) }
  }

  @Test
  fun `onStart does not reactivate manually paused partition with null pausedUntil`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "manual",
      pausedUntil = null,
    )

    recovery.onStart(startupEvent)

    verify(exactly = 0) { executionAdapter.activatePartition(any()) }
    verify(exactly = 0) { coroutinesPort.signal(any()) }
  }

  @Test
  fun `onStart immediately reactivates paused partition whose pausedUntil has already passed`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "rate_limited",
      pausedUntil = Instant.now().minusSeconds(60),
    )
    every { executionAdapter.activatePartition(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { coroutinesPort.signal(partition) }
  }

  @Test
  fun `onStart defers activation for paused partition whose pausedUntil is in the future`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "rate_limited",
      pausedUntil = Instant.now().plusSeconds(60),
    )

    recovery.onStart(startupEvent)

    verify(exactly = 0) { executionAdapter.activatePartition(any()) }
    verify(exactly = 0) { coroutinesPort.signal(any()) }
  }

  @Test
  fun `onStart handles multiple partitions independently`() {
    val partitionA = object : ApplicationOutboxPartition {
      override val key = "partition-a"
    }
    val partitionB = object : ApplicationOutboxPartition {
      override val key = "partition-b"
    }
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partitionA, partitionB)
    every { partitionPort.findOrCreate(any()) } returns OutboxPartitionInfo(
      key = "any",
      status = OutboxPartitionStatus.ACTIVE,
      statusReason = null,
      pausedUntil = null,
    )
    every { executionAdapter.activatePartition(any()) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partitionA) }
    verify { executionAdapter.activatePartition(partitionB) }
    verify { coroutinesPort.signal(partitionA) }
    verify { coroutinesPort.signal(partitionB) }
  }

  @Test
  fun `onStart recovers all task statuses before activating partitions`() {
    val activationOrder = mutableListOf<String>()
    every { executionAdapter.resetStaleProcessingTasks() } answers { activationOrder.add("reset") }
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.ACTIVE,
      statusReason = null,
      pausedUntil = null,
    )
    every { executionAdapter.activatePartition(partition) } answers { activationOrder.add("activate") }

    recovery.onStart(startupEvent)

    assertThat(activationOrder).containsExactly("reset", "activate")
  }
}
