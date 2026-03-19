package de.chrgroth.quarkus.outbox.domain

import java.time.Duration

// Interface for client applications to trigger the client applicaiton
interface ApplicationOutboxDispatcher {
  fun getAllPartitions(): List<ApplicationOutboxPartition>
  fun dispatch(event: ApplicationOutboxEvent): DispatchResult
}

sealed interface DispatchResult {
  data object Success : DispatchResult
  data class RateLimited(val retryAfter: Duration) : DispatchResult
  data class Failed(val message: String, val cause: Throwable? = null) : DispatchResult
}
