package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Any
import jakarta.enterprise.inject.Instance
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
  private val applicationPort: ApplicationPort,
  @param:Any private val partitionObservers: Instance<OutboxPartitionObserver>,
) {

  private val retryPolicy = RetryPolicy()
  private val enqueuedCounters = ConcurrentHashMap<String, Counter>()
  private val processedCounters = ConcurrentHashMap<String, Counter>()
  private val failedCounters = ConcurrentHashMap<String, Counter>()
  private val rateLimitedCounters = ConcurrentHashMap<String, Counter>()
  private val partitionStatusGauges = ConcurrentHashMap<String, AtomicInteger>()

  // --- OutboxControllerPort: enqueue ---

  fun enqueue(
    partition: ApplicationOutboxPartition,
    event: ApplicationOutboxEvent,
    payload: String,
    priority: OutboxTaskPriority,
  ): Boolean {
    val inserted = taskPort.enqueue(partition, event, payload, priority)
    if (inserted) {
      coroutinesPort.signal(partition)
      enqueuedCounters.getOrPut(partition.key) {
        meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key)
      }.increment()
    }
    return inserted
  }

  // --- OutboxControllerPort: activatePartition ---

  fun activatePartition(partition: ApplicationOutboxPartition) {
    partitionPort.resume(partition)
    getOrCreatePartitionStatusGauge(partition).set(1)
    partitionObservers.forEach { it.onPartitionActivated(partition) }
  }

  private fun pausePartition(partition: ApplicationOutboxPartition) {
    getOrCreatePartitionStatusGauge(partition).set(0)
    partitionObservers.forEach { it.onPartitionPaused(partition) }
  }

  private fun getOrCreatePartitionStatusGauge(partition: ApplicationOutboxPartition): AtomicInteger =
    partitionStatusGauges.getOrPut(partition.key) {
      val initialStatus = partitionPort.findOrCreate(partition).let {
        if (it.status == OutboxPartitionStatus.ACTIVE) 1 else 0
      }

      AtomicInteger(initialStatus).also { gauge ->
        Gauge.builder("outbox_partition_status", gauge) { it.get().toDouble() }
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

    return when (val result = applicationPort.dispatch(task)) {
      is OutboxTaskResult.Success -> {
        complete(task)
        processedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key)
        }.increment()
        true
      }

      is OutboxTaskResult.RateLimited -> {
        if (partition.pauseOnRateLimit) {
          val pausedUntil = Instant.now().plus(result.retryAfter)
          partitionPort.pause(partition, "rate_limited", pausedUntil)
          taskPort.reschedule(task, pausedUntil)
          pausePartition(partition)
        } else {
          val nextRetryAt = Instant.now().plus(result.retryAfter)
          taskPort.reschedule(task, nextRetryAt)
        }
        rateLimitedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key)
        }.increment()
        coroutinesPort.getScope().launch {
          delay(result.retryAfter.toMillis())
          if (partition.pauseOnRateLimit) {
            activatePartition(partition)
          }
          coroutinesPort.signal(partition)
        }
        false
      }

      is OutboxTaskResult.Failed -> {
        val newAttempts = task.attempts + 1
        if (newAttempts >= retryPolicy.maxAttempts) {
          fail(task, result.message, null)
        } else {
          val delay = retryPolicy.backoff.getOrElse(task.attempts) { retryPolicy.backoff.last() }
          val nextRetryAt = Instant.now().plus(delay)
          fail(task, result.message, nextRetryAt)
        }
        failedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key)
        }.increment()
        true
      }
    }
  }

  // --- OutboxRepositoryPort ---

  fun complete(task: OutboxTask) {
    archivePort.append(task)
    taskPort.delete(task)
  }

  fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?) {
    if (nextRetryAt == null) {
      archivePort.appendFailed(task, error)
      taskPort.delete(task)
    } else {
      taskPort.scheduleRetry(task, error, nextRetryAt)
    }
  }

  companion object : KLogging()
}
