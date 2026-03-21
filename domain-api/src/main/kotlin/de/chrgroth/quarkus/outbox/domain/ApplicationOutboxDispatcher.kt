package de.chrgroth.quarkus.outbox.domain

import java.time.Duration

/**
 * Outbound port implemented by the application to provide partition configuration
 * and to handle outbound dispatching of queued events.
 *
 * The outbox framework calls [getAllPartitions] at startup to discover all known partitions,
 * and calls [dispatch] for each [ApplicationOutboxEvent] that is ready to be processed.
 */
interface ApplicationOutboxDispatcher {

  /**
   * Returns all partitions that this application manages. Called once at startup.
   */
  fun getAllPartitions(): List<ApplicationOutboxPartition>

  /**
   * Dispatches the given [event] to its target destination.
   * Returns a [DispatchResult] indicating success, rate-limiting, or failure.
   */
  fun dispatch(event: ApplicationOutboxEvent): DispatchResult
}

/**
 * The result of dispatching an [ApplicationOutboxEvent].
 */
sealed interface DispatchResult {

  /** The task was dispatched successfully. */
  data object Success : DispatchResult

  /**
   * The target is currently rate-limiting requests. Processing should pause for [retryAfter]
   * before retrying.
   */
  data class RateLimited(val retryAfter: Duration) : DispatchResult

  /** Dispatching failed with an optional [cause]. */
  data class Failed(val message: String, val cause: Throwable? = null) : DispatchResult
}
