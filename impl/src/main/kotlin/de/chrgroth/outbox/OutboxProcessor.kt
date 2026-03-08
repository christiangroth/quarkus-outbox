package de.chrgroth.outbox

import java.time.Duration
import java.time.Instant

class OutboxProcessor(
    private val repository: OutboxRepository,
    private val retryPolicy: RetryPolicy = RetryPolicy(),
    private val onRateLimited: (OutboxPartition, Duration) -> Unit = { _, _ -> },
) {

    /**
     * Claims and dispatches the next available task for [partition].
     * Returns true if a task was claimed and processed (success or failure with retry).
     * Returns false if no task was available or if the partition was paused due to rate-limiting.
     */
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
}
