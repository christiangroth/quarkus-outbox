package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

/**
 * CDI event fired when an outbox partition transitions to the active state.
 *
 * This event is fired asynchronously (fire-and-forget). Client applications may
 * observe it to react to a partition becoming available for dispatch again, e.g.
 * after a pause has been lifted.
 *
 * @property partition The partition that was activated.
 */
data class OutboxPartitionActivatedEvent(val partition: ApplicationOutboxPartition)
