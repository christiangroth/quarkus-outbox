package de.chrgroth.quarkus.outbox.domain

/**
 * Converts a typed event payload [T] into its serialized [String] representation
 * for storage in the outbox.
 *
 * Implement this interface in your application and use it inside your
 * [ApplicationOutboxEvent] to produce [ApplicationOutboxEvent.serializePayload].
 */
interface OutboxPayloadSerializer<T> {

  /** Serializes [payload] to a [String] suitable for outbox storage. */
  fun serialize(payload: T): String
}
