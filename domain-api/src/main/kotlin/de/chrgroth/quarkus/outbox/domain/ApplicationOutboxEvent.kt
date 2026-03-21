package de.chrgroth.quarkus.outbox.domain

/**
 * Marker interface for domain events that can be stored in the outbox.
 *
 * Implement this interface in your application to define event types.
 * The outbox framework uses [partition] to route the event, [priority] for ordering,
 * [deduplicationKey] to prevent duplicate processing, and [serializePayload] as the
 * serialized event payload.
 *
 * Use an [OutboxPayloadSerializer] to convert your typed payload to [serializePayload].
 * When the event is dispatched by the framework, the stored payload is available via
 * [OutboxDispatchEvent.payload] and can be decoded using an [OutboxPayloadDeserializer].
 */
interface ApplicationOutboxEvent {

  /** A unique type key identifying the event type (e.g. a class name or string constant). */
  val key: String

  /** The outbox partition this event belongs to. */
  val partition: ApplicationOutboxPartition

  /** The dispatch priority for this event. */
  val priority: OutboxTaskPriority

  /** A key used to detect and discard duplicate events within the same partition. */
  val deduplicationKey: String

  /** The serialized payload of the event to store and pass to the dispatcher. */
  val serializePayload: String
}
