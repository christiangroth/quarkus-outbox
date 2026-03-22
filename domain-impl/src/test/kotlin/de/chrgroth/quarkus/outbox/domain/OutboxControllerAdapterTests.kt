package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.event.OutboxPartitionActivatedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxPartitionPausedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskDispatchedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskEnqueuedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskFailedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskRetryScheduledEvent
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import jakarta.enterprise.event.Event
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture

class OutboxControllerAdapterTests {

  private val taskPort: TaskRepositoryPort = mockk()
  private val archivePort: ArchivedTaskRepositoryPort = mockk()
  private val partitionPort: PartitionRepositoryPort = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val applicationOutboxDispatcher: ApplicationOutboxDispatcher = mockk()

  @Suppress("UNCHECKED_CAST")
  private fun <T> relaxedEventMock(): Event<T> = mockk<Event<T>>(relaxed = true).also {
    every { it.fireAsync(any()) } returns CompletableFuture.completedFuture(null as T)
  }

  private val partitionActivatedEvents: Event<OutboxPartitionActivatedEvent> = relaxedEventMock()
  private val partitionPausedEvents: Event<OutboxPartitionPausedEvent> = relaxedEventMock()
  private val taskEnqueuedEvents: Event<OutboxTaskEnqueuedEvent> = relaxedEventMock()
  private val taskDispatchedEvents: Event<OutboxTaskDispatchedEvent> = relaxedEventMock()
  private val taskRetryScheduledEvents: Event<OutboxTaskRetryScheduledEvent> = relaxedEventMock()
  private val taskFailedEvents: Event<OutboxTaskFailedEvent> = relaxedEventMock()

  private val testScope = CoroutineScope(Dispatchers.IO)
  private val coroutinesPort: CoroutinesPort = mockk {
    every { getScope() } returns testScope
    every { signal(any()) } just runs
  }

  private val adapter = OutboxControllerAdapter(
    taskPort, archivePort, partitionPort, coroutinesPort, meterRegistry, applicationOutboxDispatcher,
    partitionActivatedEvents, partitionPausedEvents,
    taskEnqueuedEvents, taskDispatchedEvents, taskRetryScheduledEvents, taskFailedEvents,
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
    priority = OutboxEventPriority.MEDIUM,
    lastError = null,
  )


  private fun testEvent() = object : ApplicationOutboxEvent {
    override val key = "TEST_EVENT"
    override val partition = this@OutboxControllerAdapterTests.partition
    override val priority = OutboxEventPriority.MEDIUM
    override val deduplicationKey = "dedup-key"
    override val serializePayload = "{}"
  }

  private fun activePartitionInfo(key: String = partition.key) = OutboxPartitionInfo(
    key = key,
    status = OutboxPartitionStatus.ACTIVE,
    statusReason = null,
    pausedUntil = null,
  )

  private fun stubDeserialize(event: ApplicationOutboxEvent = testEvent()) {
    every { applicationOutboxDispatcher.deserialize(any(), any(), any()) } returns event
  }

  // --- enqueue ---

  @Test
  fun `enqueue signals partition and increments counter when task is inserted`() {
    val event = testEvent()
    every { taskPort.enqueue(partition, any(), any(), any()) } returns true

    val result = adapter.enqueue(partition, event, "payload", OutboxEventPriority.MEDIUM)

    assertThat(result).isTrue()
    verify { coroutinesPort.signal(partition) }
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
    verify { taskEnqueuedEvents.fireAsync(OutboxTaskEnqueuedEvent(partition, event.key)) }
  }

  @Test
  fun `enqueue does not signal or increment counter when task is rejected due to deduplication`() {
    every { taskPort.enqueue(partition, any(), any(), any()) } returns false

    val result = adapter.enqueue(partition, testEvent(), "payload", OutboxEventPriority.MEDIUM)

    assertThat(result).isFalse()
    verify(exactly = 0) { coroutinesPort.signal(any()) }
    assertThat(meterRegistry.find("outbox_tasks_enqueued_all_total").counter()).isNull()
    assertThat(meterRegistry.find("outbox_tasks_enqueued_total").counter()).isNull()
    verify(exactly = 0) { taskEnqueuedEvents.fireAsync(any()) }
  }

  @Test
  fun `enqueue passes priority to repository`() {
    every { taskPort.enqueue(partition, any(), any(), OutboxEventPriority.HIGH) } returns true

    adapter.enqueue(partition, testEvent(), "payload", OutboxEventPriority.HIGH)

    verify { taskPort.enqueue(partition, any(), any(), OutboxEventPriority.HIGH) }
  }

  @Test
  fun `enqueue with HIGH priority increments high priority counter`() {
    every { taskPort.enqueue(partition, any(), any(), OutboxEventPriority.HIGH) } returns true

    adapter.enqueue(partition, testEvent(), "payload", OutboxEventPriority.HIGH)

    assertThat(meterRegistry.counter("outbox_tasks_enqueued_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key, "priority", OutboxEventPriority.HIGH.name).count()).isEqualTo(1.0)
  }

  @Test
  fun `enqueue with LOW priority increments low priority counter`() {
    every { taskPort.enqueue(partition, any(), any(), OutboxEventPriority.LOW) } returns true

    adapter.enqueue(partition, testEvent(), "payload", OutboxEventPriority.LOW)

    assertThat(meterRegistry.counter("outbox_tasks_enqueued_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key, "priority", OutboxEventPriority.LOW.name).count()).isEqualTo(1.0)
  }

  // --- activatePartition ---

  @Test
  fun `activatePartition calls repository activates gauge and fires activated event`() {
    every { partitionPort.resume(partition) } just runs
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()

    adapter.activatePartition(partition)

    verify { partitionPort.resume(partition) }
    verify { partitionActivatedEvents.fireAsync(OutboxPartitionActivatedEvent(partition)) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `activatePartition initialises gauge from persisted status when already paused`() {
    every { partitionPort.resume(partition) } just runs
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED,
      statusReason = "rate_limited",
      pausedUntil = null,
    )

    adapter.activatePartition(partition)

    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `dispatchTask pausing sets gauge to zero and fires paused event on rate limit`() {
    val task = task()
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.resume(partition) } just runs

    adapter.dispatchTask(partition)

    verify { partitionPausedEvents.fireAsync(match { it.partition == partition && it.reason == "rate_limited" }) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(0.0)
  }

  // --- dispatchTask ---

  @Test
  fun `dispatchTask returns false when no task is available`() {
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns null

    assertThat(adapter.dispatchTask(partition)).isFalse()
  }

  @Test
  fun `dispatchTask returns false immediately when partition is paused`() {
    every { partitionPort.findOrCreate(partition) } returns OutboxPartitionInfo(
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
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.Success
    every { archivePort.append(task) } just runs
    every { taskPort.delete(task) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    verify { archivePort.append(task) }
    verify { taskPort.delete(task) }
    assertThat(meterRegistry.counter("outbox_tasks_processed_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_archive_added_count").count()).isEqualTo(1.0)
    verify { taskDispatchedEvents.fireAsync(OutboxTaskDispatchedEvent(partition, task.eventType)) }
  }

  @Test
  fun `dispatchTask calls deserialize with task fields and passes result to dispatch`() {
    val task = task()
    val deserializedEvent = testEvent()
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize(deserializedEvent)
    every { applicationOutboxDispatcher.dispatch(deserializedEvent) } returns DispatchResult.Success
    every { archivePort.append(task) } just runs
    every { taskPort.delete(task) } just runs

    adapter.dispatchTask(partition)

    verify { applicationOutboxDispatcher.deserialize(partition, task.eventType, task.payload) }
    verify { applicationOutboxDispatcher.dispatch(deserializedEvent) }
  }

  @Test
  fun `dispatchTask schedules retry and increments failedCounter when below maxAttempts`() {
    val task = task(attempts = 0)
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.Failed("dispatch failed")
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.scheduleRetry(task, any(), capture(capturedNextRetryAt)) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    assertThat(capturedNextRetryAt.first()).isNotNull()
    assertThat(meterRegistry.counter("outbox_tasks_failed_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
    verify { taskRetryScheduledEvents.fireAsync(OutboxTaskRetryScheduledEvent(partition, task.eventType)) }
  }

  @Test
  fun `dispatchTask archives as failed and increments failedCounter when attempts reach maxAttempts`() {
    val task = task(attempts = 4)
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.Failed("permanent failure")
    every { archivePort.appendFailed(task, "permanent failure") } just runs
    every { taskPort.delete(task) } just runs

    assertThat(adapter.dispatchTask(partition)).isTrue()
    verify { archivePort.appendFailed(task, "permanent failure") }
    verify { taskPort.delete(task) }
    assertThat(meterRegistry.counter("outbox_tasks_failed_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_archive_added_count").count()).isEqualTo(1.0)
    verify { taskFailedEvents.fireAsync(OutboxTaskFailedEvent(partition, task.eventType)) }
  }

  @Test
  fun `dispatchTask uses backoff list correctly for retry delays`() {
    val task = task(attempts = 1)
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.Failed("fail")
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
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.Failed("fail")
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.scheduleRetry(task, any(), capture(capturedNextRetryAt)) } just runs

    val before = Instant.now()
    adapter.dispatchTask(partition)

    val captured = capturedNextRetryAt.first()
    assertThat(captured).isAfter(before.plusSeconds(59))
  }

  @Test
  fun `dispatchTask pauses partition reschedules task increments rateLimitedCounter and fires paused event`() {
    val task = task()
    every { partitionPort.findOrCreate(partition) } returns activePartitionInfo()
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.resume(partition) } just runs

    assertThat(adapter.dispatchTask(partition)).isFalse()
    verify { partitionPort.pause(partition, "rate_limited", any()) }
    verify { taskPort.reschedule(task, any()) }
    verify { partitionPausedEvents.fireAsync(match { it.partition == partition && it.reason == "rate_limited" }) }
    verify(exactly = 0) { archivePort.append(any()) }
    verify(exactly = 0) { taskPort.scheduleRetry(any(), any(), any()) }
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_all_total", "partition", partition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
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
    // findOrCreate is called three times: once for the partition status check before claiming,
    // once inside getOrCreatePartitionStatusGauge during pausePartition, and once for the
    // second dispatchTask call's partition status check which should return paused.
    every { partitionPort.findOrCreate(partition) } returnsMany listOf(activePartitionInfo(), activePartitionInfo(), pausedInfo)
    every { taskPort.claim(partition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.RateLimited(Duration.ofSeconds(30))
    every { partitionPort.pause(partition, "rate_limited", any()) } just runs
    every { taskPort.reschedule(task, any()) } just runs
    every { partitionPort.resume(partition) } just runs

    assertThat(adapter.dispatchTask(partition)).isFalse()
    assertThat(adapter.dispatchTask(partition)).isFalse()
    verify(exactly = 1) { partitionPort.pause(partition, "rate_limited", any()) }
    verify(exactly = 1) { taskPort.claim(any()) }
  }

  @Test
  fun `dispatchTask with pauseOnRateLimit=false reschedules without pausing`() {
    val task = task()
    every { partitionPort.findOrCreate(noPausePartition) } returns activePartitionInfo(noPausePartition.key)
    every { taskPort.claim(noPausePartition) } returns task
    stubDeserialize()
    every { applicationOutboxDispatcher.dispatch(any()) } returns DispatchResult.RateLimited(Duration.ofSeconds(30))
    val capturedNextRetryAt = mutableListOf<Instant>()
    every { taskPort.reschedule(task, capture(capturedNextRetryAt)) } just runs

    assertThat(adapter.dispatchTask(noPausePartition)).isFalse()
    verify(exactly = 0) { partitionPort.pause(any(), any(), any()) }
    verify { taskPort.reschedule(task, any()) }
    assertThat(capturedNextRetryAt.first()).isAfter(Instant.now().plusSeconds(28))
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_all_total", "partition", noPausePartition.key).count()).isEqualTo(1.0)
    assertThat(meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", noPausePartition.key, "priority", OutboxEventPriority.MEDIUM.name).count()).isEqualTo(1.0)
  }
}
