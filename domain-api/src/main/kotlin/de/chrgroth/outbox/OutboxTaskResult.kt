package de.chrgroth.outbox

import java.time.Duration

sealed interface OutboxTaskResult {
    data object Success : OutboxTaskResult
    data class RateLimited(val retryAfter: Duration) : OutboxTaskResult
    data class Failed(val message: String, val cause: Throwable? = null) : OutboxTaskResult
}
