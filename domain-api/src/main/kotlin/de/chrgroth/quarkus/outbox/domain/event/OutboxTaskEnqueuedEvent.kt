package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox task is successfully enqueued.
 *
 * This event is fired asynchronously (fire-and-forget). Tasks that are silently
 * discarded due to deduplication do not produce this event.
 *
 * @property partition The partition the task was enqueued into.
 * @property eventType The type key of the enqueued event.
 */
data class OutboxTaskEnqueuedEvent(
    val partition: ApplicationOutboxPartition,
    val eventType: String,
)
