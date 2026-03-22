package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import java.time.Instant

/**
 * CDI event fired when an outbox partition transitions to the paused state.
 *
 * This event is fired asynchronously (fire-and-forget). Client applications may
 * observe it to react to pause conditions.
 *
 * @property partition The partition that was paused.
 * @property reason An optional short string describing why the partition was paused.
 * @property pausedUntil The time at which the partition is scheduled to resume, or `null` if unknown.
 */
data class OutboxPartitionPausedEvent(
    val partition: ApplicationOutboxPartition,
    val reason: String?,
    val pausedUntil: Instant?,
)
