package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox task has been rescheduled because the partition was paused.
 *
 * This event is fired asynchronously (fire-and-forget). The task remains in the
 * pending queue and will be dispatched once the partition is resumed.
 *
 * @property partition The partition the task belongs to.
 * @property eventType The type key of the rescheduled event.
 */
data class OutboxTaskRescheduledEvent(
    val partition: ApplicationOutboxPartition,
    val eventType: String,
)
