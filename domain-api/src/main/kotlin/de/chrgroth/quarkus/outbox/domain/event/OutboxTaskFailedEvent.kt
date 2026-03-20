package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.OutboxTask

/**
 * CDI event fired when an outbox task has permanently failed after exhausting all retries.
 *
 * This event is fired asynchronously (fire-and-forget). The task has been moved to the
 * archive as [OutboxTask.status] = `FAILED` and will not be retried.
 *
 * @property task The task that has permanently failed.
 * @property error A description of the final failure.
 */
data class OutboxTaskFailedEvent(
    val task: OutboxTask,
    val error: String,
)
