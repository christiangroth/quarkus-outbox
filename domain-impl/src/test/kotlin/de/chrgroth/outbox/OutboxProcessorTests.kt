package de.chrgroth.outbox

import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class OutboxProcessorTests {

    private val repository: OutboxRepository = mockk()
    private val retryPolicy = RetryPolicy(
        maxAttempts = 3,
        backoff = listOf(Duration.ofSeconds(5), Duration.ofSeconds(10)),
    )
    private val processor = OutboxProcessor(repository, retryPolicy)

    private val partition = object : OutboxPartition {
        override val key = "test-partition"
    }

    private val noPausePartition = object : OutboxPartition {
        override val key = "test-partition-no-pause"
        override val pauseOnRateLimit = false
    }

    private fun task(attempts: Int = 0) = OutboxTask(
        id = "task-1",
        partition = partition.key,
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

    @Test
    fun `processNext returns false when no task is available`() {
        every { repository.claim(partition) } returns null

        val result = processor.processNext(partition) { OutboxTaskResult.Success }

        assertThat(result).isFalse()
    }

    @Test
    fun `processNext calls complete on successful dispatch`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.complete(task) } just runs

        val result = processor.processNext(partition) { OutboxTaskResult.Success }

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

        val result = processor.processNext(partition) { OutboxTaskResult.Failed("dispatch failed") }

        assertThat(result).isTrue()
        assertThat(capturedNextRetryAt.first()).isNotNull()
    }

    @Test
    fun `processNext calls fail with null nextRetryAt when attempts reach maxAttempts to trigger archiving`() {
        val task = task(attempts = 2) // maxAttempts=3, so newAttempts=3 >= maxAttempts
        every { repository.claim(partition) } returns task
        every { repository.fail(task, any(), null) } just runs

        val result = processor.processNext(partition) { OutboxTaskResult.Failed("permanent failure") }

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
        processor.processNext(partition) { OutboxTaskResult.Failed("fail") }
        val after = Instant.now()

        // nextRetryAt should be approximately now + 10 seconds
        val captured = capturedNextRetryAt.first()!!
        assertThat(captured).isAfter(before.plusSeconds(9))
        assertThat(captured).isBefore(after.plusSeconds(11))
    }

    @Test
    fun `processNext uses last backoff entry when attempts exceed backoff list`() {
        val task = task(attempts = 5) // beyond backoff list size
        val largeRetryPolicy = RetryPolicy(
            maxAttempts = 10,
            backoff = listOf(Duration.ofSeconds(5), Duration.ofSeconds(10)),
        )
        val processorWithLargePolicy = OutboxProcessor(repository, largeRetryPolicy)
        every { repository.claim(partition) } returns task
        val capturedNextRetryAt = mutableListOf<Instant?>()
        every { repository.fail(task, any(), captureNullable(capturedNextRetryAt)) } just runs

        val before = Instant.now()
        processorWithLargePolicy.processNext(partition) { OutboxTaskResult.Failed("fail") }

        // Should use last backoff entry (10s) for out-of-bounds index
        val captured = capturedNextRetryAt.first()!!
        assertThat(captured).isAfter(before.plusSeconds(9))
    }

    @Test
    fun `processNext returns true when task was claimed even on dispatch failure`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.fail(task, any(), any()) } just runs

        val result = processor.processNext(partition) { OutboxTaskResult.Failed("error") }

        assertThat(result).isTrue()
    }

    @Test
    fun `processNext pauses partition and reschedules task without incrementing attempts on rate limited result`() {
        val task = task(attempts = 1)
        every { repository.claim(partition) } returns task
        every { repository.pausePartition(partition, "rate_limited", any()) } just runs
        every { repository.reschedule(task, any()) } just runs

        val retryAfter = Duration.ofSeconds(30)
        val result = processor.processNext(partition) { OutboxTaskResult.RateLimited(retryAfter) }

        assertThat(result).isFalse()
        verify { repository.pausePartition(partition, "rate_limited", any()) }
        verify { repository.reschedule(task, any()) }
        verify(exactly = 0) { repository.complete(any()) }
        verify(exactly = 0) { repository.fail(any(), any(), any()) }
    }

    @Test
    fun `processNext schedules resume via onRateLimited callback with correct retryAfter`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.pausePartition(partition, "rate_limited", any()) } just runs
        every { repository.reschedule(task, any()) } just runs

        val capturedPartitions = mutableListOf<OutboxPartition>()
        val capturedDurations = mutableListOf<Duration>()
        val processorWithCallback = OutboxProcessor(repository, retryPolicy) { p, d ->
            capturedPartitions.add(p)
            capturedDurations.add(d)
        }

        val retryAfter = Duration.ofSeconds(60)
        processorWithCallback.processNext(partition) { OutboxTaskResult.RateLimited(retryAfter) }

        assertThat(capturedPartitions).containsExactly(partition)
        assertThat(capturedDurations).containsExactly(retryAfter)
    }

    @Test
    fun `rate limited task blocks all subsequent tasks in the partition`() {
        val task = task()
        // First claim returns a task; second returns null as the repository reflects the paused partition state
        every { repository.claim(partition) } returnsMany listOf(task, null)
        every { repository.pausePartition(partition, "rate_limited", any()) } just runs
        every { repository.reschedule(task, any()) } just runs

        // First call: dispatch signals rate-limited – partition is paused, processNext returns false
        val firstResult = processor.processNext(partition) { OutboxTaskResult.RateLimited(Duration.ofSeconds(30)) }
        assertThat(firstResult).isFalse()

        // Second call: claim returns null (partition paused) – no further tasks processed
        val secondResult = processor.processNext(partition) { OutboxTaskResult.Success }
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
        val result = processor.processNext(noPausePartition) { OutboxTaskResult.RateLimited(retryAfter) }

        assertThat(result).isFalse()
        verify(exactly = 0) { repository.pausePartition(any(), any(), any()) }
        verify { repository.reschedule(task, any()) }
        val captured = capturedNextRetryAt.first()
        assertThat(captured).isAfter(Instant.now().plusSeconds(28))
    }

    @Test
    fun `processNext with pauseOnRateLimit=false invokes onRateLimited callback for delayed wakeup`() {
        val task = task()
        every { repository.claim(noPausePartition) } returns task
        every { repository.reschedule(task, any()) } just runs

        val callbackInvoked = mutableListOf<OutboxPartition>()
        val processorWithCallback = OutboxProcessor(repository, retryPolicy) { p, _ -> callbackInvoked.add(p) }

        processorWithCallback.processNext(noPausePartition) { OutboxTaskResult.RateLimited(Duration.ofSeconds(30)) }

        assertThat(callbackInvoked).containsExactly(noPausePartition)
    }
}
