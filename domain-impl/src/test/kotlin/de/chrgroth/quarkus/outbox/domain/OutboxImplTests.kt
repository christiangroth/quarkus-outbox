package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.OutboxPartitionObserver
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import jakarta.enterprise.inject.Instance
import kotlinx.coroutines.channels.Channel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Instant

class OutboxImplTests {

    private val repository: OutboxRepository = mockk()
    private val wakeupService: OutboxWakeupService = mockk()
    private val meterRegistry = SimpleMeterRegistry()
    private val partitionObserver: OutboxPartitionObserver = mockk(relaxed = true)

    @Suppress("UNCHECKED_CAST")
    private val partitionObservers: Instance<OutboxPartitionObserver> = mockk<Instance<OutboxPartitionObserver>>().also {
        every { it.iterator() } answers { mutableListOf(partitionObserver).iterator() }
    }

    private val partition = object : OutboxPartition {
        override val key = "test-partition"
    }

    private val noPausePartition = object : OutboxPartition {
        override val key = "no-pause-partition"
        override val pauseOnRateLimit = false
    }

    private val outbox = OutboxImpl(repository, wakeupService, meterRegistry, partitionObservers)

    @AfterEach
    fun tearDown() {
        outbox.onStop()
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
        every { wakeupService.signal(partition) } just runs

        val result = outbox.enqueue(partition, testEvent(), "payload")

        assertThat(result).isTrue()
        verify { wakeupService.signal(partition) }
        assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key).count()).isEqualTo(1.0)
    }

    @Test
    fun `enqueue does not signal or increment counter when task is rejected due to deduplication`() {
        every { repository.enqueue(partition, any(), any(), any()) } returns false

        val result = outbox.enqueue(partition, testEvent(), "payload")

        assertThat(result).isFalse()
        verify(exactly = 0) { wakeupService.signal(any()) }
        assertThat(meterRegistry.find("outbox_tasks_enqueued_total").counter()).isNull()
    }

    @Test
    fun `enqueue passes priority to repository`() {
        every { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) } returns true
        every { wakeupService.signal(partition) } just runs

        outbox.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.HIGH)

        verify { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) }
    }

    // --- processNext ---

    @Test
    fun `processNext increments processedCounter on success`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.complete(task) } just runs

        val result = outbox.processNext(partition) { OutboxTaskResult.Success }

        assertThat(result).isTrue()
        assertThat(meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key).count()).isEqualTo(1.0)
    }

    @Test
    fun `processNext increments failedCounter on failure`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.fail(task, any(), any()) } just runs

        val result = outbox.processNext(partition) { OutboxTaskResult.Failed("error") }

        assertThat(result).isTrue()
        assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key).count()).isEqualTo(1.0)
    }

    @Test
    fun `processNext increments rateLimitedCounter and pauses partition status gauge on rate limited`() {
        val task = task()
        every { repository.claim(partition) } returns task
        every { repository.findPartition(partition) } returns null
        every { repository.pausePartition(partition, any(), any()) } just runs
        every { repository.reschedule(task, any()) } just runs

        outbox.processNext(partition) { OutboxTaskResult.RateLimited(java.time.Duration.ofSeconds(30)) }

        assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key).count()).isEqualTo(1.0)
        assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(0.0)
    }

    @Test
    fun `processNext increments rateLimitedCounter but does not update gauge when pauseOnRateLimit is false`() {
        val task = task(noPausePartition.key)
        every { repository.claim(noPausePartition) } returns task
        every { repository.reschedule(task, any()) } just runs

        outbox.processNext(noPausePartition) { OutboxTaskResult.RateLimited(java.time.Duration.ofSeconds(30)) }

        assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", noPausePartition.key).count()).isEqualTo(1.0)
        assertThat(meterRegistry.find("outbox_partition_status").tag("partition", noPausePartition.key).gauge()).isNull()
    }

    @Test
    fun `processNext returns false when no task is available`() {
        every { repository.claim(partition) } returns null

        val result = outbox.processNext(partition) { OutboxTaskResult.Success }

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

    // --- internal methods ---

    @Test
    fun `signal delegates to wakeupService`() {
        every { wakeupService.signal(partition) } just runs

        outbox.signal(partition)

        verify { wakeupService.signal(partition) }
    }

    @Test
    fun `getOrCreateChannel delegates to wakeupService`() {
        val channel: Channel<Unit> = mockk()
        every { wakeupService.getOrCreate(partition) } returns channel

        val result = outbox.getOrCreateChannel(partition)

        assertThat(result).isSameAs(channel)
    }

    @Test
    fun `resetStaleProcessingTasks delegates to repository`() {
        every { repository.resetStaleProcessingTasks() } just runs

        outbox.resetStaleProcessingTasks()

        verify { repository.resetStaleProcessingTasks() }
    }

    @Test
    fun `findPartition delegates to repository`() {
        val info = OutboxPartitionInfo(partition.key, OutboxPartitionStatus.ACTIVE.name, null, null)
        every { repository.findPartition(partition) } returns info

        val result = outbox.findPartition(partition)

        assertThat(result).isEqualTo(info)
    }

    @Test
    fun `archiveFailedTasks delegates to repository`() {
        every { repository.archiveFailedTasks() } returns 5L

        val result = outbox.archiveFailedTasks()

        assertThat(result).isEqualTo(5L)
    }
}
