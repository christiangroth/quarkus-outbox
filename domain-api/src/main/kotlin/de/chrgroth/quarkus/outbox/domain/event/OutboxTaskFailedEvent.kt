package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox task has permanently failed after exhausting all retries.
 *
 * This event is fired asynchronously (fire-and-forget). The task has been moved to the
 * archive as `FAILED` and will not be retried.
 *
 * @property partition The partition the task belonged to.
 * @property eventType The type key of the failed event.
 */
data class OutboxTaskFailedEvent(
    val partition: ApplicationOutboxPartition,
    val eventType: String,
)
