package de.chrgroth.quarkus.outbox.domain

/**
 * Dispatch priority for outbox events. Higher-priority events are processed before normal ones.
 */
enum class OutboxEventPriority {
  NORMAL,
  HIGH,
}

/**
 * Marker interface for domain events that can be stored in the outbox.
 *
 * Implement this interface in your application to define event types.
 * The outbox framework uses [partition] to route the event, [priority] for ordering,
 * [deduplicationKey] to prevent duplicate processing, and [serializePayload] as the
 * serialized event payload.
 *
 * When the framework is ready to dispatch a stored event it calls
 * [ApplicationOutboxDispatcher.deserialize] to reconstruct an instance of this interface
 * from the stored data, then passes it to [ApplicationOutboxDispatcher.dispatch].
 */
interface ApplicationOutboxEvent {

  /** A unique type key identifying the event type (e.g. a class name or string constant). */
  val key: String

  /** The outbox partition this event belongs to. */
  val partition: ApplicationOutboxPartition

  /** The dispatch priority for this event. */
  val priority: OutboxEventPriority

  /** A key used to detect and discard duplicate events within the same partition. */
  val deduplicationKey: String

  /** The serialized payload of the event to store and pass to the dispatcher. */
  val serializePayload: String
}
