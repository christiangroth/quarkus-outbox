package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

/**
 * Outbound port implemented by the application to provide partition configuration,
 * payload deserialization, and outbound dispatching of queued events.
 *
 * The outbox framework calls [getAllPartitions] at startup to discover all known partitions.
 * For each event ready to be processed, the framework first calls [deserialize] to
 * reconstruct the [ApplicationOutboxEvent] from stored data, then calls [dispatch] with
 * the result.
 */
interface ApplicationOutboxDispatcher {

  /**
   * Returns all partitions that this application manages. Called once at startup.
   */
  fun getAllPartitions(): List<ApplicationOutboxPartition>

  /**
   * Reconstructs an [ApplicationOutboxEvent] from the stored [partition], [eventType] and
   * serialized [payload]. The returned event is passed directly to [dispatch].
   */
  fun deserialize(partition: ApplicationOutboxPartition, eventType: String, payload: String): ApplicationOutboxEvent

  /**
   * Dispatches the given [event] to its target destination.
   * Returns a [DispatchResult] indicating success, a requested pause, or failure.
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
   * Processing should be paused, optionally until [pausedUntil].
   * An optional [reason] can be provided to describe why the pause was requested.
   */
  data class Paused(val reason: String? = null, val pausedUntil: Instant? = null) : DispatchResult

  /** Dispatching failed with an optional [cause]. */
  data class Failed(val message: String, val cause: Throwable? = null) : DispatchResult
}
