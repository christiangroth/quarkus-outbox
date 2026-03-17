package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.ExecutionPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class ExecutionAdapter(
  private val coroutinesAdapter: CoroutinesAdapter,
  private val repository: OutboxRepository,
  private val meterRegistry: MeterRegistry,
  private val partitionAdapter: PartitionAdapter,
) : ExecutionPort {

  private val retryPolicy = RetryPolicy()
  private val onRateLimited: (OutboxPartition, Duration) -> Unit = { partition, retryAfter ->
    coroutinesAdapter.scope().launch {
      delay(retryAfter.toMillis())
      if (partition.pauseOnRateLimit) {
        partitionAdapter.activatePartition(partition)
      }
      coroutinesAdapter.wakeUp(partition)
    }
  }

  private val processedCounters = ConcurrentHashMap<String, Counter>()
  private val failedCounters = ConcurrentHashMap<String, Counter>()
  private val rateLimitedCounters = ConcurrentHashMap<String, Counter>()

  fun dispatchNext(partition: OutboxPartition, dispatch: (OutboxTask) -> OutboxTaskResult): Boolean =
    processNext(partition) { task ->
      val result = dispatch(task)
      when (result) {
        is OutboxTaskResult.Success -> processedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key)
        }.increment()

        is OutboxTaskResult.RateLimited -> {
          rateLimitedCounters.getOrPut(partition.key) {
            meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key)
          }.increment()
          if (partition.pauseOnRateLimit) {
            partitionAdapter.pausePartition(partition)
          }
        }

        is OutboxTaskResult.Failed -> failedCounters.getOrPut(partition.key) {
          meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key)
        }.increment()
      }
      result
    }

  fun processNext(
    partition: OutboxPartition,
    dispatch: (OutboxTask) -> OutboxTaskResult,
  ): Boolean {
    val task = repository.claim(partition) ?: return false

    return when (val result = dispatch(task)) {
      is OutboxTaskResult.Success -> {
        repository.complete(task)
        true
      }

      is OutboxTaskResult.RateLimited -> {
        if (partition.pauseOnRateLimit) {
          val pausedUntil = Instant.now().plus(result.retryAfter)
          repository.pausePartition(partition, "rate_limited", pausedUntil)
          repository.reschedule(task, pausedUntil)
        } else {
          val nextRetryAt = Instant.now().plus(result.retryAfter)
          repository.reschedule(task, nextRetryAt)
        }
        onRateLimited(partition, result.retryAfter)
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
        true
      }
    }
  }

  fun resetStaleProcessingTasks() = repository.resetStaleProcessingTasks()

  override fun archiveFailedTasks() = repository.archiveFailedTasks()
}

