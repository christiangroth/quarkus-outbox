package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxPartitionObserver
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxTaskResult
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class OutboxControllerAdapterTests {

  private val taskPort: TaskRepositoryPort = mockk()
  private val archivePort: ArchivedTaskRepositoryPort = mockk()
  private val partitionPort: PartitionRepositoryPort = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val applicationPort: ApplicationPort = mockk()
  private val partitionObserver: OutboxPartitionObserver = mockk(relaxed = true)

  @Suppress("UNCHECKED_CAST")
  private val partitionObservers: Instance<OutboxPartitionObserver> = mockk<Instance<OutboxPartitionObserver>>().also {
    every { it.iterator() } answers { mutableListOf(partitionObserver).iterator() }
  }

  private val testScope = CoroutineScope(Dispatchers.IO)
  private val coroutinesPort: CoroutinesPort = mockk {
    every { getScope() } returns testScope
    every { signal(any()) } just runs
  }

  private val adapter = OutboxControllerAdapter(
    taskPort, archivePort, partitionPort, coroutinesPort, meterRegistry, applicationPort, partitionObservers,
  )

  private val partition = object : ApplicationOutboxPartition {
    override val key = "test-partition"
  }

  private val noPausePartition = object : ApplicationOutboxPartition {
    override val key = "no-pause-partition"
    override val pauseOnRateLimit = false
  }

  @AfterEach
  fun tearDown() {
    testScope.cancel()
  }

  private fun task(partitionKey: String = partition.key, attempts: Int = 0) = OutboxTask(
    id = "task-1",
    partition = partitionKey,
    eventType = "TEST_EVENT",
    payload = """{"foo":"bar"}""",
    deduplicationKey = "dedup-1",
    status = OutboxTaskStatus.PROCESSING,
    attempts = attempts,
    createdAt = Instant.now(),
    updatedAt = Instant.now(),
    nextRetryAt = null,
    priority = OutboxTaskPriority.NORMAL,
    lastError = null,
  )

  private fun testEvent() = object : ApplicationOutboxEvent {
    override val key = "TEST_EVENT"
    override fun deduplicationKey() = "dedup-key"
  }

  // --- enqueue ---

  @Test
  fun `enqueue signals partition and increments counter when task is inserted`() {
    every { taskPort.enqueue(partition, any(), any(), any()) } returns true

    val result = adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isTrue()
    verify { coroutinesPort.signal(partition) }
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `enqueue does not signal or increment counter when task is rejected due to deduplication`() {
    every { taskPort.enqueue(partition, any(), any(), any()) } returns false

    val result = adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isFalse()
    verify(exactly = 0) { coroutinesPort.signal(any()) }
    assertThat(meterRegistry.find("outbox_tasks_enqueued_total").counter()).isNull()
  }

  @Test
  fun `enqueue passes priority to repository`() {
    every { taskPort.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) } returns true

    adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.HIGH)

    verify { taskPort.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) }
  }

  // --- activatePartition ---

  @Test
  fun `activatePartition calls repository activates gauge and notifies observers`() {
    every { partitionPort.activate(partition) } just runs
    every { partitionPort.findPartition(partition.key) } returns null

    adapter.activatePartition(partition)

    verify { partitionPort.activate(partition) }
    verify { partitionObserver.onPartitionActivated(partition) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `activatePartition initialises gauge from persisted status when already paused`() {
    every { partitionPort.activate(partition) } just runs
    every { partitionPort.findPartition(partition.key) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "rate_limited",
      pausedUntil = null,
    )

    adapter.activatePartition(partition)

    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask pausing sets gauge to zero and notifies observers on rate limit`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.activate(partition) } just runs

    adapter.dispatchTask(partition)

    verify { partitionObserver.onPartitionPaused(partition) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(0.0)
  }

  // --- dispatchTask ---

  @Test
  fun `dispatchTask returns false when no task is available`() {
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns null

    assertThat(adapter.dispatchTask(partition)).isFalse()
  }

  @Test
  fun `dispatchTask returns false immediately when partition is paused`() {
    every { partitionPort.findPartition(partition.key) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "manual",
      pausedUntil = null,
    )

    assertThat(adapter.dispatchTask(partition)).isFalse()
    verify(exactly = 0) { taskPort.claim(any()) }
  }

  @Test
  fun `dispatchTask archives task and increments processedCounter on success`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Success
    every { archivePort.append(task) } just runs
    every { taskPort.delete(task) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    verify { archivePort.append(task) }
    verify { taskPort.delete(task) }
    assertThat(meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask schedules retry and increments failedCounter when below maxAttempts`() {
    val task = task(attempts = 0)
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("dispatch failed")
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.scheduleRetry(task, any(), capture(capturedNextRetryAt)) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    assertThat(capturedNextRetryAt.first()).isNotNull()
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask archives as failed and increments failedCounter when attempts reach maxAttempts`() {
    val task = task(attempts = 4)
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("permanent failure")
    every { archivePort.appendFailed(task, "permanent failure") } just runs
    every { taskPort.delete(task) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    verify { archivePort.appendFailed(task, "permanent failure") }
    verify { taskPort.delete(task) }
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask uses backoff list correctly for retry delays`() {
    val task = task(attempts = 1)
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("fail")
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.scheduleRetry(task, any(), capture(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    adapter.dispatchTask(partition)
    val after = Instant.now()

    val captured = capturedNextRetryAt.first()
    assertThat(captured).isAfter(before.plusSeconds(9))
    assertThat(captured).isBefore(after.plusSeconds(11))
  }

  @Test
  fun `dispatchTask uses last backoff entry when attempt index reaches end of backoff list`() {
    val task = task(attempts = 3)
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("fail")
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.scheduleRetry(task, any(), capture(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    adapter.dispatchTask(partition)

    val captured = capturedNextRetryAt.first()
    assertThat(captured).isAfter(before.plusSeconds(59))
  }

  @Test
  fun `dispatchTask returns true when task was claimed even on dispatch failure`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("error")
    every { taskPort.scheduleRetry(task, any(), any()) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
  }

  @Test
  fun `dispatchTask pauses partition reschedules task increments rateLimitedCounter and notifies observers`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.activate(partition) } just runs

    assertThat(adapter.dispatchTask(partition)).isFalse()
    verify { partitionPort.pause(partition, "rate_limited", any()) }
    verify { taskPort.reschedule(task, any()) }
    verify { partitionObserver.onPartitionPaused(partition) }
    verify(exactly = 0) { archivePort.append(any()) }
    verify(exactly = 0) { taskPort.scheduleRetry(any(), any(), any()) }
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `rate limited task blocks all subsequent tasks in the partition`() {
    val task = task()
    val pausedInfo = OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "rate_limited",
      pausedUntil = null,
    )
    every { partitionPort.findPartition(partition.key) } returnsMany listOf(null, pausedInfo)
    every { taskPort.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.activate(partition) } just runs

    assertThat(adapter.dispatchTask(partition)).isFalse()
    assertThat(adapter.dispatchTask(partition)).isFalse()
    verify(exactly = 1) { partitionPort.pause(partition, "rate_limited", any()) }
    verify(exactly = 1) { taskPort.claim(any()) }
  }

  @Test
  fun `dispatchTask with pauseOnRateLimit=false reschedules without pausing`() {
    val task = task()
    every { partitionPort.findPartition(noPausePartition.key) } returns null
    every { taskPort.claim(noPausePartition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.reschedule(task, capture(capturedNextRetryAt)) } just runs

    assertThat(adapter.dispatchTask(noPausePartition)).isFalse()
    verify(exactly = 0) { partitionPort.pause(any(), any(), any()) }
    verify { taskPort.reschedule(task, any()) }
    assertThat(capturedNextRetryAt.first()).isAfter(Instant.now().plusSeconds(28))
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", noPausePartition.key).count()).isEqualTo(1.0)
  }

  // --- resetStaleProcessingTasks ---

  @Test
  fun `resetStaleProcessingTasks delegates to taskPort`() {
    every { taskPort.resetStaleProcessing() } just runs

    adapter.resetStaleProcessingTasks()

    verify { taskPort.resetStaleProcessing() }
  }

  // --- complete ---

  @Test
  fun `complete archives task and deletes it`() {
    val task = task()
    every { archivePort.append(task) } just runs
    every { taskPort.delete(task) } just runs

    adapter.complete(task)

    verify { archivePort.append(task) }
    verify { taskPort.delete(task) }
  }

  // --- fail ---

  @Test
  fun `fail with null nextRetryAt appends as failed and deletes task`() {
    val task = task()
    every { archivePort.appendFailed(task, "error") } just runs
    every { taskPort.delete(task) } just runs

    adapter.fail(task, "error", null)

    verify { archivePort.appendFailed(task, "error") }
    verify { taskPort.delete(task) }
    verify(exactly = 0) { taskPort.scheduleRetry(any(), any(), any()) }
  }

  @Test
  fun `fail with nextRetryAt schedules retry`() {
    val task = task()
    val nextRetryAt = Instant.now().plusSeconds(30)
    every { taskPort.scheduleRetry(task, "error", nextRetryAt) } just runs

    adapter.fail(task, "error", nextRetryAt)

    verify { taskPort.scheduleRetry(task, "error", nextRetryAt) }
    verify(exactly = 0) { archivePort.appendFailed(any(), any()) }
  }
}
