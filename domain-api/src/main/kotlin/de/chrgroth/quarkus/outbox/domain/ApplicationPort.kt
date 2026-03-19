package de.chrgroth.quarkus.outbox.domain

import java.time.Duration

interface ApplicationPort {
  fun getAllPartitions(): List<ApplicationOutboxPartition>
  fun dispatch(task: OutboxTask): OutboxTaskResult
}

sealed interface OutboxTaskResult {
    data object Success : OutboxTaskResult
    data class RateLimited(val retryAfter: Duration) : OutboxTaskResult
    data class Failed(val message: String, val cause: Throwable? = null) : OutboxTaskResult
}
