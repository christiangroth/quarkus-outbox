package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchivePort
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionAdapter
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class ArchiveAdapter(
  private val coroutinesPort: CoroutinesPort,
  private val repository: OutboxRepository,
  private val meterRegistry: MeterRegistry,
  private val partitionAdapter: PartitionAdapter,
  private val applicationPort: ApplicationPort,
) : ArchivePort {

  private val retryPolicy = RetryPolicy()
  private val processedCounters = ConcurrentHashMap<String, Counter>()
  private val failedCounters = ConcurrentHashMap<String, Counter>()
  private val rateLimitedCounters = ConcurrentHashMap<String, Counter>()

  fun dispatchTask(partition: OutboxPartition): Boolean {
    val task = repository.claim(partition) ?: return false

    return when (val result = applicationPort.dispatch(task)) {
      is OutboxTaskResult.Success -> {
        repository.complete(task)
        processedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key)
        }.increment()
        true
      }

      is OutboxTaskResult.RateLimited -> {
        if (partition.pauseOnRateLimit) {
          val pausedUntil = Instant.now().plus(result.retryAfter)
          repository.pausePartition(partition, "rate_limited", pausedUntil)
          repository.reschedule(task, pausedUntil)
          partitionAdapter.pausePartition(partition)
        } else {
          val nextRetryAt = Instant.now().plus(result.retryAfter)
          repository.reschedule(task, nextRetryAt)
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
          repository.fail(task, result.message, null)
        } else {
          val delay = retryPolicy.backoff.getOrElse(task.attempts) { retryPolicy.backoff.last() }
          val nextRetryAt = Instant.now().plus(delay)
          repository.fail(task, result.message, nextRetryAt)
        }
        failedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key)
        }.increment()
        true
      }
    }
  }

  fun resetStaleProcessingTasks() = repository.resetStaleProcessingTasks()

  override fun archiveFailedTasks() = repository.archiveFailedTasks()
}

