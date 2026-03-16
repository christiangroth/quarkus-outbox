package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.OutboxPartitionObserver
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.spyk
import io.mockk.verify
import jakarta.enterprise.inject.Instance
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class ExecutionAdapterTests {

  private val repository: OutboxRepository = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val partitionObserver: OutboxPartitionObserver = mockk(relaxed = true)

  @Suppress("UNCHECKED_CAST")
  private val partitionObservers: Instance<OutboxPartitionObserver> = mockk<Instance<OutboxPartitionObserver>>().also {
    every { it.iterator() } answers { mutableListOf(partitionObserver).iterator() }
  }

  private val coroutinesAdapter = spyk(CoroutinesAdapter())

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  private val noPausePartition = object : OutboxPartition {
    override val key = "no-pause-partition"
    override val pauseOnRateLimit = false
  }

  private val outbox = ExecutionAdapter(coroutinesAdapter, repository, meterRegistry, partitionObservers)

  @AfterEach
  fun tearDown() {
    coroutinesAdapter.onStop()
  }

  private fun testEvent() = object : OutboxEvent {
    override val key = "TEST_EVENT"
    override fun deduplicationKey() = "dedup-key"
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

  // --- enqueue ---

  @Test
  fun `enqueue signals partition and increments counter when task is inserted`() {
    every { repository.enqueue(partition, any(), any(), any()) } returns true

    val result = outbox.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isTrue()
    verify { coroutinesAdapter.wakeUp(partition) }
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `enqueue does not signal or increment counter when task is rejected due to deduplication`() {
    every { repository.enqueue(partition, any(), any(), any()) } returns false

    val result = outbox.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isFalse()
    verify(exactly = 0) { coroutinesAdapter.wakeUp(any()) }
    assertThat(meterRegistry.find("outbox_tasks_enqueued_total").counter()).isNull()
  }

  @Test
  fun `enqueue passes priority to repository`() {
    every { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) } returns true

    outbox.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.HIGH)

    verify { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) }
  }

  // --- processNext ---

  @Test
  fun `processNext returns false when no task is available`() {
    every { repository.claim(partition) } returns null

    val result = outbox.processNext(partition) { OutboxTaskResult.Success }

    assertThat(result).isFalse()
  }

  @Test
  fun `processNext calls complete on successful dispatch`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { repository.complete(task) } just runs

    val result = outbox.processNext(partition) { OutboxTaskResult.Success }

    assertThat(result).isTrue()
    verify { repository.complete(task) }
    verify(exactly = 0) { repository.fail(any(), any(), any()) }
  }

  @Test
  fun `processNext calls fail with retry time when dispatch fails and attempts below maxAttempts`() {
    val task = task(attempts = 0)
    every { repository.claim(partition) } returns task
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val result = outbox.processNext(partition) { OutboxTaskResult.Failed("dispatch failed") }

    assertThat(result).isTrue()
    assertThat(capturedNextRetryAt.first()).isNotNull()
  }

  @Test
  fun `processNext calls fail with null nextRetryAt when attempts reach maxAttempts`() {
    val task = task(attempts = 4) // default maxAttempts=5, newAttempts=5 >= maxAttempts
    every { repository.claim(partition) } returns task
    every { repository.fail(task, any(), null) } just runs

    val result = outbox.processNext(partition) { OutboxTaskResult.Failed("permanent failure") }

    assertThat(result).isTrue()
    verify { repository.fail(task, "permanent failure", null) }
  }

  @Test
  fun `processNext uses backoff list correctly for retry delays`() {
    val task = task(attempts = 1) // second attempt -> use backoff[1] = 10s
    every { repository.claim(partition) } returns task
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    outbox.processNext(partition) { OutboxTaskResult.Failed("fail") }
    val after = Instant.now()

    val captured = capturedNextRetryAt.first()!!
    assertThat(captured).isAfter(before.plusSeconds(9))
    assertThat(captured).isBefore(after.plusSeconds(11))
  }

  @Test
  fun `processNext uses last backoff entry when attempt index reaches end of backoff list`() {
    val task = task(attempts = 3) // index 3 = last entry in default backoff = 60s, newAttempts=4 < maxAttempts=5
    every { repository.claim(partition) } returns task
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    outbox.processNext(partition) { OutboxTaskResult.Failed("fail") }

    val captured = capturedNextRetryAt.first()!!
    assertThat(captured).isAfter(before.plusSeconds(59))
  }

  @Test
  fun `processNext returns true when task was claimed even on dispatch failure`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { repository.fail(task, any(), any()) } just runs

    val result = outbox.processNext(partition) { OutboxTaskResult.Failed("error") }

    assertThat(result).isTrue()
  }

  @Test
  fun `processNext pauses partition and reschedules task without incrementing attempts on rate limited result`() {
    val task = task(attempts = 1)
    every { repository.claim(partition) } returns task
    every { repository.pausePartition(partition, "rate_limited", any()) } just runs
    every { repository.reschedule(task, any()) } just runs

    val retryAfter = Duration.ofSeconds(30)
    val result = outbox.processNext(partition) { OutboxTaskResult.RateLimited(retryAfter) }

    assertThat(result).isFalse()
    verify { repository.pausePartition(partition, "rate_limited", any()) }
    verify { repository.reschedule(task, any()) }
    verify(exactly = 0) { repository.complete(any()) }
    verify(exactly = 0) { repository.fail(any(), any(), any()) }
  }

  @Test
  fun `rate limited task blocks all subsequent tasks in the partition`() {
    val task = task()
    every { repository.claim(partition) } returnsMany listOf(task, null)
    every { repository.pausePartition(partition, "rate_limited", any()) } just runs
    every { repository.reschedule(task, any()) } just runs

    val firstResult = outbox.processNext(partition) { OutboxTaskResult.RateLimited(Duration.ofSeconds(30)) }
    assertThat(firstResult).isFalse()

    val secondResult = outbox.processNext(partition) { OutboxTaskResult.Success }
    assertThat(secondResult).isFalse()

    verify(exactly = 1) { repository.pausePartition(partition, "rate_limited", any()) }
  }

  @Test
  fun `processNext with pauseOnRateLimit=false reschedules task without pausing partition`() {
    val task = task(attempts = 1)
    every { repository.claim(noPausePartition) } returns task
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { repository.reschedule(task, capture(capturedNextRetryAt)) } just runs

    val retryAfter = Duration.ofSeconds(30)
    val result = outbox.processNext(noPausePartition) { OutboxTaskResult.RateLimited(retryAfter) }

    assertThat(result).isFalse()
    verify(exactly = 0) { repository.pausePartition(any(), any(), any()) }
    verify { repository.reschedule(task, any()) }
    val captured = capturedNextRetryAt.first()
    assertThat(captured).isAfter(Instant.now().plusSeconds(28))
  }

  // --- dispatchNext ---

  @Test
  fun `dispatchNext increments processedCounter on success`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { repository.complete(task) } just runs

    val result = outbox.dispatchNext(partition) { OutboxTaskResult.Success }

    assertThat(result).isTrue()
    assertThat(meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchNext increments failedCounter on failure`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { repository.fail(task, any(), any()) } just runs

    val result = outbox.dispatchNext(partition) { OutboxTaskResult.Failed("error") }

    assertThat(result).isTrue()
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchNext increments rateLimitedCounter and pauses partition status gauge on rate limited`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { repository.findPartition(partition) } returns null
    every { repository.pausePartition(partition, any(), any()) } just runs
    every { repository.reschedule(task, any()) } just runs

    outbox.dispatchNext(partition) { OutboxTaskResult.RateLimited(Duration.ofSeconds(30)) }

    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(0.0)
  }

  @Test
  fun `dispatchNext increments rateLimitedCounter but does not update gauge when pauseOnRateLimit is false`() {
    val task = task(noPausePartition.key)
    every { repository.claim(noPausePartition) } returns task
    every { repository.reschedule(task, any()) } just runs

    outbox.dispatchNext(noPausePartition) { OutboxTaskResult.RateLimited(Duration.ofSeconds(30)) }

    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", noPausePartition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", noPausePartition.key).gauge()).isNull()
  }

  @Test
  fun `dispatchNext returns false when no task is available`() {
    every { repository.claim(partition) } returns null

    val result = outbox.dispatchNext(partition) { OutboxTaskResult.Success }

    assertThat(result).isFalse()
  }

  // --- activatePartition ---

  @Test
  fun `activatePartition calls repository activates gauge and notifies observers`() {
    every { repository.activatePartition(partition) } just runs
    every { repository.findPartition(partition) } returns null

    outbox.activatePartition(partition)

    verify { repository.activatePartition(partition) }
    verify { partitionObserver.onPartitionActivated(partition) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `activatePartition initialises gauge from persisted status when already paused`() {
    every { repository.activatePartition(partition) } just runs
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "rate_limited",
      pausedUntil = null,
    )

    outbox.activatePartition(partition)

    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  // --- resetStaleProcessingTasks ---

  @Test
  fun `resetStaleProcessingTasks delegates to repository`() {
    every { repository.resetStaleProcessingTasks() } just runs

    outbox.resetStaleProcessingTasks()

    verify { repository.resetStaleProcessingTasks() }
  }

  // --- archiveFailedTasks ---

  @Test
  fun `archiveFailedTasks delegates to repository`() {
    every { repository.archiveFailedTasks() } returns 5L

    val result = outbox.archiveFailedTasks()

    assertThat(result).isEqualTo(5L)
  }
}
