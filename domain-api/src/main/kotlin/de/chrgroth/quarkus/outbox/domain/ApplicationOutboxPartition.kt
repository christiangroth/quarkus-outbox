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

  /**
   * Number of concurrent worker coroutines started for this partition.
   *
   * Tasks are routed to workers by hashing their [ApplicationOutboxEvent.groupId] modulo
   * this worker count, so two tasks with the same `groupId` are always claimed by the same
   * worker and therefore processed strictly sequentially in enqueue order. Tasks without a
   * `groupId` are always routed to worker `0`, preserving today's single global ordering for
   * ungrouped tasks even when [workerCount] is greater than `1`.
   *
   * Defaults to `1`, matching the existing single-worker-per-partition behavior. Values less
   * than `1` are treated as `1`.
   */
  val workerCount: Int get() = 1
}
