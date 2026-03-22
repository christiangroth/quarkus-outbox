package de.chrgroth.quarkus.outbox.domain

import java.time.Duration

/**
 * Represents a logical partition of the outbox. All tasks within the same partition
 * are processed sequentially and share a common pause and throttle configuration.
 *
 * Implement this interface (typically as an enum or sealed class) to define the
 * partitions used by your application.
 */
interface ApplicationOutboxPartition {
  val key: String

  /**
   * Minimum delay between consecutive task dispatches for this partition.
   * Set to a positive [Duration] to proactively throttle outgoing requests.
   * Defaults to `null` (no throttling).
   */
  val throttleInterval: Duration? get() = null
}
