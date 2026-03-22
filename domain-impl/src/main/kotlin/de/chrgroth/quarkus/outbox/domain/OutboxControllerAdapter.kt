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
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Event
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@ApplicationScoped
class OutboxControllerAdapter(
  private val taskPort: TaskRepositoryPort,
  private val archivePort: ArchivedTaskRepositoryPort,
  private val partitionPort: PartitionRepositoryPort,
  private val coroutinesPort: CoroutinesPort,
  private val meterRegistry: MeterRegistry,
  private val applicationOutboxDispatcher: ApplicationOutboxDispatcher,
  private val partitionActivatedEvents: Event<OutboxPartitionActivatedEvent>,
  private val partitionPausedEvents: Event<OutboxPartitionPausedEvent>,
  private val taskEnqueuedEvents: Event<OutboxTaskEnqueuedEvent>,
  private val taskDispatchedEvents: Event<OutboxTaskDispatchedEvent>,
  private val taskRetryScheduledEvents: Event<OutboxTaskRetryScheduledEvent>,
  private val taskFailedEvents: Event<OutboxTaskFailedEvent>,
) {

  private val retryPolicy = RetryPolicy()
  private val enqueuedCounters = ConcurrentHashMap<String, Counter>()
  private val processedCounters = ConcurrentHashMap<String, Counter>()
  private val failedCounters = ConcurrentHashMap<String, Counter>()
  private val pausedCounters = ConcurrentHashMap<String, Counter>()
  private val partitionStatusGauges = ConcurrentHashMap<String, AtomicInteger>()
  private val archivedTasksAddedCounter = meterRegistry.counter("outbox.archive.added")

  // --- OutboxControllerPort: enqueue ---

  fun enqueue(
    partition: ApplicationOutboxPartition,
    event: ApplicationOutboxEvent,
    payload: String,
    priority: OutboxEventPriority,
  ): Boolean {
    val inserted = taskPort.enqueue(partition, event, payload, priority)
    if (inserted) {
      coroutinesPort.signal(partition)
      enqueuedCounters.getOrPut("${partition.key}:${priority.name}") {
        meterRegistry.counter("outbox.tasks.enqueued", "partition", partition.key, "priority", priority.name)
      }.increment()
      taskEnqueuedEvents.fireAsync(OutboxTaskEnqueuedEvent(partition, event.key))
    }
    return inserted
  }

  // --- OutboxControllerPort: activatePartition ---

  fun activatePartition(partition: ApplicationOutboxPartition) {
    partitionPort.resume(partition)
    getOrCreatePartitionStatusGauge(partition).set(1)
    partitionActivatedEvents.fireAsync(OutboxPartitionActivatedEvent(partition))
  }

  private fun pausePartition(partition: ApplicationOutboxPartition, reason: String?, pausedUntil: Instant?) {
    getOrCreatePartitionStatusGauge(partition).set(0)
    partitionPausedEvents.fireAsync(OutboxPartitionPausedEvent(partition, reason, pausedUntil))
  }

  private fun getOrCreatePartitionStatusGauge(partition: ApplicationOutboxPartition): AtomicInteger =
    partitionStatusGauges.getOrPut(partition.key) {
      val initialStatus = partitionPort.findOrCreate(partition).let {
        if (it.status == OutboxPartitionStatus.ACTIVE) 1 else 0
      }

      AtomicInteger(initialStatus).also { gauge ->
        Gauge.builder("outbox.partition.status", gauge) { it.get().toDouble() }
          .tag("partition", partition.key)
          .description("Outbox partition status: 1=active, 0=paused")
          .register(meterRegistry)
      }
    }

  // --- Dispatch ---

  fun resetStaleProcessingTasks() = taskPort.resetStaleProcessing()

  fun dispatchTask(partition: ApplicationOutboxPartition): Boolean {
    val partitionInfo = partitionPort.findOrCreate(partition)
    if (partitionInfo.status == OutboxPartitionStatus.PAUSED) {
      return false
    }

    val task = taskPort.claim(partition)
      ?: return false

    val event = applicationOutboxDispatcher.deserialize(partition, task.eventType, task.payload)
    return when (val result = applicationOutboxDispatcher.dispatch(event)) {
      is DispatchResult.Success -> {
        complete(task, partition)
        processedCounters.getOrPut("${partition.key}:${task.priority.name}") {
          meterRegistry.counter("outbox.tasks.processed", "partition", partition.key, "priority", task.priority.name)
        }.increment()
        true
      }

      is DispatchResult.Paused -> {
        val pausedUntil = result.pausedUntil
        partitionPort.pause(partition, result.reason, pausedUntil)
        taskPort.reschedule(task, pausedUntil ?: Instant.now())
        pausePartition(partition, result.reason, pausedUntil)
        pausedCounters.getOrPut("${partition.key}:${task.priority.name}") {
          meterRegistry.counter("outbox.tasks.paused", "partition", partition.key, "priority", task.priority.name)
        }.increment()
        if (pausedUntil != null) {
          val delayMs = maxOf(0L, pausedUntil.toEpochMilli() - Instant.now().toEpochMilli())
          coroutinesPort.getScope().launch {
            delay(delayMs)
            activatePartition(partition)
            coroutinesPort.signal(partition)
          }
        }
        false
      }

      is DispatchResult.Failed -> {
        val newAttempts = task.attempts + 1
        if (newAttempts >= retryPolicy.maxAttempts) {
          fail(task, result.message, null, partition)
        } else {
          val delay = retryPolicy.backoff.getOrElse(task.attempts) { retryPolicy.backoff.last() }
          val nextRetryAt = Instant.now().plus(delay)
          fail(task, result.message, nextRetryAt, partition)
        }
        failedCounters.getOrPut("${partition.key}:${task.priority.name}") {
          meterRegistry.counter("outbox.tasks.failed", "partition", partition.key, "priority", task.priority.name)
        }.increment()
        true
      }
    }
  }

  // --- OutboxRepositoryPort ---

  fun complete(task: OutboxTask, partition: ApplicationOutboxPartition) {
    archivePort.append(task)
    archivedTasksAddedCounter.increment()
    taskPort.delete(task)
    taskDispatchedEvents.fireAsync(OutboxTaskDispatchedEvent(partition, task.eventType))
  }

  fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?, partition: ApplicationOutboxPartition) {
    if (nextRetryAt == null) {
      archivePort.appendFailed(task, error)
      archivedTasksAddedCounter.increment()
      taskPort.delete(task)
      taskFailedEvents.fireAsync(OutboxTaskFailedEvent(partition, task.eventType))
    } else {
      taskPort.scheduleRetry(task, error, nextRetryAt)
      taskRetryScheduledEvents.fireAsync(OutboxTaskRetryScheduledEvent(partition, task.eventType))
    }
  }

  companion object : KLogging()
}
