package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox task has been successfully dispatched.
 *
 * This event is fired asynchronously (fire-and-forget). The task has been archived
 * as `DONE` and removed from the pending queue.
 *
 * @property partition The partition the task belonged to.
 * @property eventType The type key of the dispatched event.
 */
data class OutboxTaskDispatchedEvent(
    val partition: ApplicationOutboxPartition,
    val eventType: String,
)
