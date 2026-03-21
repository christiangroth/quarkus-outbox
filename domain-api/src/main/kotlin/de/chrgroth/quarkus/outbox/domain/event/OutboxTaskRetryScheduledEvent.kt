package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox task has failed dispatch but will be retried.
 *
 * This event is fired asynchronously (fire-and-forget). The task remains in the
 * pending queue and will be retried automatically.
 *
 * @property partition The partition the task belongs to.
 * @property eventType The type key of the event scheduled for retry.
 */
data class OutboxTaskRetryScheduledEvent(
    val partition: ApplicationOutboxPartition,
    val eventType: String,
)
