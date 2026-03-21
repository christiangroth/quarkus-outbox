package de.chrgroth.quarkus.outbox.domain

/**
 * Reconstructs a typed event payload [T] from its serialized [String] representation
 * stored in the outbox.
 *
 * Implement this interface in your application and use it inside your
 * [ApplicationOutboxDispatcher.dispatch] implementation to decode
 * [OutboxDispatchEvent.payload] back into its original type.
 */
interface OutboxPayloadDeserializer<T> {

  /** Deserializes [payload] back into a typed [T] instance. */
  fun deserialize(payload: String): T
}
