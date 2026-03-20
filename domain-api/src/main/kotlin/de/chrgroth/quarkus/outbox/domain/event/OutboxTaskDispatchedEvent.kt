package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.OutboxTask

/**
 * CDI event fired when an outbox task has been successfully dispatched.
 *
 * This event is fired asynchronously (fire-and-forget). The task has been archived
 * as [OutboxTask.status] = `DONE` and removed from the pending queue.
 *
 * @property task The task that was dispatched successfully.
 */
data class OutboxTaskDispatchedEvent(val task: OutboxTask)
