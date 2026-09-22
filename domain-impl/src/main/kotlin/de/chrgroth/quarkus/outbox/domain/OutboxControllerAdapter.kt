package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.event.OutboxPartitionActivatedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxPartitionPausedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskDispatchedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskEnqueuedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskFailedEvent
import de.chrgroth.quarkus.outbox.domain.event.OutboxTaskRescheduledEvent
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
import org.eclipse.microprofile.config.inject.ConfigProperty
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
  private val taskRescheduledEvents: Event<OutboxTaskRescheduledEvent>,
  @param:ConfigProperty(name = "outbox.archive.enabled", defaultValue = "true")
  private val archiveEnabled: Boolean,
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
    notBefore: Instant? = null,
  ): Boolean {
    val inserted = taskPort.enqueue(partition, event, payload, priority, notBefore)
    if (inserted) {
      partitionPort.incrementEventTypeCount(partition, event.key)
      coroutinesPort.signal(partition)
      if (notBefore != null && notBefore.isAfter(Instant.now())) {
        scheduleSignalAt(partition, notBefore)
      }
      enqueuedCounters.getOrPut("${partition.key}:${priority.name}") {
        meterRegistry.counter("outbox.tasks.enqueued", "partition", partition.key, "priority", priority.name)
      }.increment()
      taskEnqueuedEvents.fireAsync(OutboxTaskEnqueuedEvent(partition, event.key))
    }
    return inserted
  }

  // --- OutboxControllerPort: cancel / reschedule by deduplication key ---

  fun cancel(partition: ApplicationOutboxPartition, deduplicationKey: String): Boolean {
    val cancelled = taskPort.cancelByDeduplicationKey(partition, deduplicationKey) ?: return false
    partitionPort.decrementEventTypeCount(partition, cancelled.eventType)
    logger.info { "Cancelled pending task ${cancelled.id} (partition=${partition.key}, deduplicationKey=$deduplicationKey)" }
    return true
  }

  fun reschedule(partition: ApplicationOutboxPartition, deduplicationKey: String, notBefore: Instant?): Boolean {
    val rescheduled = taskPort.rescheduleByDeduplicationKey(partition, deduplicationKey, notBefore) ?: return false
    if (notBefore != null && notBefore.isAfter(Instant.now())) {
      scheduleSignalAt(partition, notBefore)
    } else {
      coroutinesPort.signal(partition)
    }
    logger.info { "Rescheduled pending task ${rescheduled.id} (partition=${partition.key}, deduplicationKey=$deduplicationKey) to $notBefore" }
    return true
  }

  // --- OutboxControllerPort: activatePartition ---

  fun activatePartition(partition: ApplicationOutboxPartition) {
    partitionPort.resume(partition)
    getOrCreatePartitionStatusGauge(partition).set(1)
    partitionActivatedEvents.fireAsync(OutboxPartitionActivatedEvent(partition))
  }

  fun scheduleRetryWakeupIfNeeded(partition: ApplicationOutboxPartition) {
    val nextRetryAt = taskPort.findEarliestPendingRetryAt(partition) ?: return
    scheduleSignalAt(partition, nextRetryAt)
  }

  fun scheduleDelayedWakeupIfNeeded(partition: ApplicationOutboxPartition) {
    val nextNotBeforeAt = taskPort.findEarliestPendingNotBeforeAt(partition) ?: return
    scheduleSignalAt(partition, nextNotBeforeAt)
  }

  private fun pausePartition(partition: ApplicationOutboxPartition, reason: String?, pausedUntil: Instant?) {
    getOrCreatePartitionStatusGauge(partition).set(0)
    logger.warn {
      "Pausing partition ${partition.key}" +
        (reason?.let { ", reason: $it" } ?: "") +
        (pausedUntil?.let { ", until $it" } ?: ", indefinitely")
    }
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

  @Suppress("TooGenericExceptionCaught")
  fun dispatchTask(partition: ApplicationOutboxPartition): Boolean {
    val partitionInfo = partitionPort.findOrCreate(partition)
    if (partitionInfo.status == OutboxPartitionStatus.PAUSED) {
      return false
    }

    val task = taskPort.claim(partition)
      ?: return false

    val dispatchResult = try {
      val event = applicationOutboxDispatcher.deserialize(partition, task.eventType, task.payload)
      applicationOutboxDispatcher.dispatch(event)
    } catch (e: Exception) {
      logger.error(e) { "Unexpected error dispatching task ${task.id} for partition ${partition.key}, treating as failed" }
      DispatchResult.Failed(e.message ?: e.toString(), e)
    }

    return when (dispatchResult) {
      is DispatchResult.Success -> {
        complete(task, partition)
        processedCounters.getOrPut("${partition.key}:${task.priority.name}") {
          meterRegistry.counter("outbox.tasks.processed", "partition", partition.key, "priority", task.priority.name)
        }.increment()
        true
      }

      is DispatchResult.Paused -> {
        val pausedUntil = dispatchResult.pausedUntil
        partitionPort.pause(partition, dispatchResult.reason, pausedUntil)
        taskPort.reschedule(task, pausedUntil ?: Instant.now())
        taskRescheduledEvents.fireAsync(OutboxTaskRescheduledEvent(partition, task.eventType))
        pausePartition(partition, dispatchResult.reason, pausedUntil)
        pausedCounters.getOrPut("${partition.key}:${task.priority.name}") {
          meterRegistry.counter("outbox.tasks.paused", "partition", partition.key, "priority", task.priority.name)
        }.increment()
        if (pausedUntil != null) {
          val delayMs = maxOf(0L, pausedUntil.toEpochMilli() - Instant.now().toEpochMilli())
          coroutinesPort.getScope().launch {
            delay(delayMs)
            logger.info { "Resuming partition ${partition.key} after pause expired" }
            activatePartition(partition)
            coroutinesPort.signal(partition)
          }
        }
        false
      }

      is DispatchResult.Failed -> {
        val newAttempts = task.attempts + 1
        if (newAttempts >= retryPolicy.maxAttempts) {
          fail(task, dispatchResult.message, null, partition)
        } else {
          val delay = retryPolicy.backoff.getOrElse(task.attempts) { retryPolicy.backoff.last() }
          val nextRetryAt = Instant.now().plus(delay)
          fail(task, dispatchResult.message, nextRetryAt, partition)
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
    if (archiveEnabled) {
      archivePort.append(task)
      archivedTasksAddedCounter.increment()
    }
    taskPort.delete(task)
    partitionPort.decrementEventTypeCount(partition, task.eventType)
    taskDispatchedEvents.fireAsync(OutboxTaskDispatchedEvent(partition, task.eventType))
  }

  fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?, partition: ApplicationOutboxPartition) {
    if (nextRetryAt == null) {
      logger.warn { "Task ${task.id} (partition=${partition.key}, event=${task.eventType}) permanently failed after ${task.attempts + 1} attempt(s): $error" }
      if (archiveEnabled) {
        archivePort.appendFailed(task, error)
        archivedTasksAddedCounter.increment()
      }
      taskPort.delete(task)
      partitionPort.decrementEventTypeCount(partition, task.eventType)
      taskFailedEvents.fireAsync(OutboxTaskFailedEvent(partition, task.eventType))
    } else {
      logger.info {
        "Task ${task.id} (partition=${partition.key}, event=${task.eventType}) failed, retry scheduled at $nextRetryAt " +
          "(attempt ${task.attempts + 1}): $error"
      }
      taskPort.scheduleRetry(task, error, nextRetryAt)
      taskRetryScheduledEvents.fireAsync(OutboxTaskRetryScheduledEvent(partition, task.eventType))
      scheduleSignalAt(partition, nextRetryAt)
    }
  }

  /** Schedules a coroutine that signals [partition] once [at] is reached (retry, delayed dispatch, or reschedule). */
  private fun scheduleSignalAt(partition: ApplicationOutboxPartition, at: Instant) {
    val delayMs = maxOf(0L, at.toEpochMilli() - Instant.now().toEpochMilli())
    coroutinesPort.getScope().launch {
      delay(delayMs)
      coroutinesPort.signal(partition)
    }
  }

  companion object : KLogging()
}
