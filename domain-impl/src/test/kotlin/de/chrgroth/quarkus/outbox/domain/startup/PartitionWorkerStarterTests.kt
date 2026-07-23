package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxDispatcher
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxControllerAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import io.mockk.coEvery
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import io.quarkus.runtime.StartupEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class PartitionWorkerStarterTests {

  private val testScope = CoroutineScope(Dispatchers.IO)
  private val coroutinesPort: CoroutinesPort = mockk(relaxUnitFun = true) {
    every { getScope() } returns testScope
  }
  private val partitionPort: PartitionRepositoryPort = mockk()
  private val executionAdapter: OutboxControllerAdapter = mockk()
  private val application: ApplicationOutboxDispatcher = mockk()

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
    every { executionAdapter.scheduleRetryWakeupIfNeeded(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { executionAdapter.scheduleRetryWakeupIfNeeded(partition) }
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
      statusReason = "my-reason",
      pausedUntil = Instant.now().minusSeconds(60),
    )
    every { executionAdapter.activatePartition(partition) } just runs
    every { executionAdapter.scheduleRetryWakeupIfNeeded(partition) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partition) }
    verify { executionAdapter.scheduleRetryWakeupIfNeeded(partition) }
    verify { coroutinesPort.signal(partition) }
  }

  @Test
  fun `onStart defers activation for paused partition whose pausedUntil is in the future`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "my-reason",
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
    every { executionAdapter.scheduleRetryWakeupIfNeeded(any()) } just runs

    recovery.onStart(startupEvent)

    verify { executionAdapter.activatePartition(partitionA) }
    verify { executionAdapter.activatePartition(partitionB) }
    verify { executionAdapter.scheduleRetryWakeupIfNeeded(partitionA) }
    verify { executionAdapter.scheduleRetryWakeupIfNeeded(partitionB) }
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
    every { executionAdapter.scheduleRetryWakeupIfNeeded(partition) } just runs

    recovery.onStart(startupEvent)

    assertThat(activationOrder).containsExactly("reset", "activate")
  }

  @Test
  fun `partition worker survives exception from dispatchTask and waits for next signal`() {
    every { executionAdapter.resetStaleProcessingTasks() } just runs
    every { application.getAllPartitions() } returns listOf(partition)
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.ACTIVE,
      statusReason = null,
      pausedUntil = null,
    )
    every { executionAdapter.activatePartition(partition) } just runs
    every { executionAdapter.scheduleRetryWakeupIfNeeded(partition) } just runs

    val waitCount = AtomicInteger(0)
    coEvery { coroutinesPort.waitOnSignal(partition) } answers { waitCount.incrementAndGet(); Unit }

    val dispatchCount = AtomicInteger(0)
    every { executionAdapter.dispatchTask(partition) } answers {
      if (dispatchCount.getAndIncrement() == 0) throw IllegalStateException("boom") else false
    }

    recovery.onStart(startupEvent)

    runBlocking {
      withTimeout(2000) {
        while (waitCount.get() < 2) {
          delay(10)
        }
      }
    }

    assertThat(waitCount.get()).isGreaterThanOrEqualTo(2)
    assertThat(testScope.isActive).isTrue()
  }
}
