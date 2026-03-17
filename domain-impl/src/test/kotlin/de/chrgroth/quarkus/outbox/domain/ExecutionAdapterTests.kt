package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionAdapter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class ExecutionAdapterTests {

  private val repository: OutboxRepository = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val partitionAdapter: PartitionAdapter = mockk(relaxed = true)
  private val applicationPort: ApplicationPort = mockk()

  private val testScope = CoroutineScope(Dispatchers.IO)
  private val coroutinesPort: CoroutinesPort = mockk {
    every { scope() } returns testScope
    every { wakeUp(any()) } just runs
  }

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  private val noPausePartition = object : OutboxPartition {
    override val key = "no-pause-partition"
    override val pauseOnRateLimit = false
  }

  private val outbox = ExecutionAdapter(coroutinesPort, repository, meterRegistry, partitionAdapter, applicationPort)

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

  // --- dispatchTask ---

  @Test
  fun `dispatchTask returns false when no task is available`() {
    every { repository.claim(partition) } returns null

    val result = outbox.dispatchTask(partition)

    assertThat(result).isFalse()
  }

  @Test
  fun `dispatchTask calls complete and increments processedCounter on success`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Success
    every { repository.complete(task) } just runs

    val result = outbox.dispatchTask(partition)

    assertThat(result).isTrue()
    verify { repository.complete(task) }
    verify(exactly = 0) { repository.fail(any(), any(), any()) }
    assertThat(meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask calls fail with retry time and increments failedCounter when below maxAttempts`() {
    val task = task(attempts = 0)
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("dispatch failed")
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val result = outbox.dispatchTask(partition)

    assertThat(result).isTrue()
    assertThat(capturedNextRetryAt.first()).isNotNull()
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask calls fail with null nextRetryAt and increments failedCounter when attempts reach maxAttempts`() {
    val task = task(attempts = 4)
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("permanent failure")
    every { repository.fail(task, any(), null) } just runs

    val result = outbox.dispatchTask(partition)

    assertThat(result).isTrue()
    verify { repository.fail(task, "permanent failure", null) }
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask uses backoff list correctly for retry delays`() {
    val task = task(attempts = 1) // second attempt -> use backoff[1] = 10s
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("fail")
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    outbox.dispatchTask(partition)
    val after = Instant.now()

    val captured = capturedNextRetryAt.first()!!
    assertThat(captured).isAfter(before.plusSeconds(9))
    assertThat(captured).isBefore(after.plusSeconds(11))
  }

  @Test
  fun `dispatchTask uses last backoff entry when attempt index reaches end of backoff list`() {
    val task = task(attempts = 3) // index 3 = last entry in default backoff = 60s
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("fail")
    val capturedNextRetryAt = mutableListOf<Instant?>()
    every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    outbox.dispatchTask(partition)

    val captured = capturedNextRetryAt.first()!!
    assertThat(captured).isAfter(before.plusSeconds(59))
  }

  @Test
  fun `dispatchTask returns true when task was claimed even on dispatch failure`() {
    val task = task()
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.Failed("error")
    every { repository.fail(task, any(), any()) } just runs

    val result = outbox.dispatchTask(partition)

    assertThat(result).isTrue()
  }

  @Test
  fun `dispatchTask pauses partition reschedules task increments rateLimitedCounter and delegates pause`() {
    val task = task(attempts = 1)
    every { repository.claim(partition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    every { repository.pausePartition(partition, "rate_limited", any()) } just runs
    every { repository.reschedule(task, any()) } just runs

    val result = outbox.dispatchTask(partition)

    assertThat(result).isFalse()
    verify { repository.pausePartition(partition, "rate_limited", any()) }
    verify { repository.reschedule(task, any()) }
    verify { partitionAdapter.pausePartition(partition) }
    verify(exactly = 0) { repository.complete(any()) }
    verify(exactly = 0) { repository.fail(any(), any(), any()) }
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `rate limited task blocks all subsequent tasks in the partition`() {
    val task = task()
    every { repository.claim(partition) } returnsMany listOf(task, null)
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    every { repository.pausePartition(partition, "rate_limited", any()) } just runs
    every { repository.reschedule(task, any()) } just runs

    val firstResult = outbox.dispatchTask(partition)
    assertThat(firstResult).isFalse()

    val secondResult = outbox.dispatchTask(partition)
    assertThat(secondResult).isFalse()

    verify(exactly = 1) { repository.pausePartition(partition, "rate_limited", any()) }
  }

  @Test
  fun `dispatchTask with pauseOnRateLimit=false reschedules without pausing and does not delegate pause`() {
    val task = task(attempts = 1)
    every { repository.claim(noPausePartition) } returns task
    every { applicationPort.dispatch(task) } returns OutboxTaskResult.RateLimited(Duration.ofSeconds(30))
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { repository.reschedule(task, capture(capturedNextRetryAt)) } just runs

    val result = outbox.dispatchTask(noPausePartition)

    assertThat(result).isFalse()
    verify(exactly = 0) { repository.pausePartition(any(), any(), any()) }
    verify(exactly = 0) { partitionAdapter.pausePartition(any()) }
    verify { repository.reschedule(task, any()) }
    val captured = capturedNextRetryAt.first()
    assertThat(captured).isAfter(Instant.now().plusSeconds(28))
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", noPausePartition.key).count()).isEqualTo(1.0)
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

