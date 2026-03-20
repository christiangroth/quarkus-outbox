package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.OutboxTask
import java.time.Instant

/**
 * CDI event fired when an outbox task has failed dispatch but will be retried.
 *
 * This event is fired asynchronously (fire-and-forget). The task remains in the
 * pending queue and will be retried at [nextRetryAt].
 *
 * @property task The task that failed and is scheduled for retry.
 * @property error A description of the failure that triggered the retry.
 * @property nextRetryAt The time at which the task will next be attempted.
 */
data class OutboxTaskRetryScheduledEvent(
    val task: OutboxTask,
    val error: String,
    val nextRetryAt: Instant,
)
