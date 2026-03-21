package de.chrgroth.quarkus.outbox.domain.event

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import java.time.Instant

/**
 * CDI event fired when an outbox partition transitions to the paused state.
 *
 * This event is fired asynchronously (fire-and-forget). Client applications may
 * observe it to react to rate-limiting or other pause conditions.
 *
 * @property partition The partition that was paused.
 * @property reason A short string describing why the partition was paused (e.g. `"rate_limited"`).
 * @property pausedUntil The time at which the partition is scheduled to resume, or `null` if unknown.
 */
data class OutboxPartitionPausedEvent(
    val partition: ApplicationOutboxPartition,
    val reason: String,
    val pausedUntil: Instant?,
)
