package de.chrgroth.quarkus.outbox.domain

/**
 * Represents the event data to be dispatched by the outbox framework.
 *
 * An [OutboxEvent] is derived from a stored [OutboxTask] and carries only the
 * event-relevant information needed by the application to perform the actual dispatch.
 * Internal tracking fields such as task id, status, or retry state are kept internal
 * to the framework.
 */
data class OutboxEvent(
    val eventType: String,
    val partition: String,
    val payload: String,
    val priority: OutboxTaskPriority,
    val deduplicationKey: String,
)
