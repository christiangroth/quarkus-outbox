package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionAdapter
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class OutboxControllerAdapter(
  private val taskPort: TaskRepositoryPort,
  private val archivePort: ArchivedTaskRepositoryPort,
  private val partitionPort: PartitionRepositoryPort,
  private val coroutinesPort: CoroutinesPort,
  private val meterRegistry: MeterRegistry,
  private val partitionAdapter: PartitionAdapter,
  private val applicationPort: ApplicationPort,
) : OutboxRepositoryPort {

  private val retryPolicy = RetryPolicy()
  private val processedCounters = ConcurrentHashMap<String, Counter>()
  private val failedCounters = ConcurrentHashMap<String, Counter>()
  private val rateLimitedCounters = ConcurrentHashMap<String, Counter>()

  fun resetStaleProcessingTasks() = taskPort.resetStaleProcessing()

  fun dispatchTask(partition: OutboxPartition): Boolean {
    val partitionInfo = partitionPort.findPartition(partition.key)
    if (partitionInfo != null && partitionInfo.status == OutboxPartitionStatus.PAUSED.name) {
      return false
    }
    val task = taskPort.claim(partition) ?: return false

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
          partitionAdapter.pausePartition(partition)
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
            partitionAdapter.activatePartition(partition)
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

  override fun complete(task: OutboxTask) {
    archivePort.append(task)
    taskPort.delete(task)
  }

  override fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?) {
    if (nextRetryAt == null) {
      archivePort.appendFailed(task, error)
      taskPort.delete(task)
    } else {
      taskPort.scheduleRetry(task, error, nextRetryAt)
    }
  }

  override fun archiveFailedTasks(): Long {
    val failedTasks = taskPort.listFailed()
    if (failedTasks.isEmpty()) return 0L
    var count = 0L
    for (task in failedTasks) {
      archivePort.upsertFailed(task)
      taskPort.delete(task)
      count++
    }
    logger.info { "Archived $count failed outbox tasks" }
    return count
  }

  companion object : KLogging()
}
