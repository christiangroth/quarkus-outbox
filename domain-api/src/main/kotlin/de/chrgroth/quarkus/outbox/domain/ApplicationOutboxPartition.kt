package de.chrgroth.quarkus.outbox.domain

import java.time.Duration

/**
 * Represents a logical partition of the outbox. All tasks within the same partition
 * are processed sequentially and share a common rate-limit and throttle configuration.
 *
 * Implement this interface (typically as an enum or sealed class) to define the
 * partitions used by your application.
 */
interface ApplicationOutboxPartition {
  val key: String

  /**
   * Controls whether the partition is paused when a rate-limited response is received.
   * Set to `false` for partitions where processing must continue even under rate limiting
   * (e.g. to avoid missing time-sensitive data). Defaults to `true`.
   */
  val pauseOnRateLimit: Boolean get() = true

  /**
   * Minimum delay between consecutive task dispatches for this partition.
   * Set to a positive [Duration] to proactively throttle outgoing requests and avoid rate limiting.
   * Defaults to `null` (no throttling).
   */
  val throttleInterval: Duration? get() = null
}
