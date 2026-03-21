package de.chrgroth.quarkus.outbox.domain

/**
 * Represents the event data handed to [ApplicationOutboxDispatcher.dispatch] when the
 * outbox framework is ready to deliver a stored event.
 *
 * An [OutboxDispatchEvent] is derived from a stored [OutboxTask] and contains only the
 * event-relevant information needed by the application to perform the actual dispatch.
 * Internal tracking fields (task id, status, retry state, …) remain internal to the
 * framework and are never exposed here.
 *
 * Use an [OutboxPayloadDeserializer] in your [ApplicationOutboxDispatcher.dispatch]
 * implementation to decode [payload] back into the typed object your application needs.
 */
data class OutboxDispatchEvent(
    val eventType: String,
    val partition: ApplicationOutboxPartition,
    val payload: String,
    val priority: OutboxTaskPriority,
    val deduplicationKey: String,
)
