package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

/**
 * Snapshot of the current state of an outbox partition, including status,
 * optional pause reason, and task count information.
 */
data class OutboxPartitionInfo(
  val key: String,
  val status: OutboxPartitionStatus,
  val statusReason: String?,
  val pausedUntil: Instant?,
  val eventCount: Long? = null,
  val eventPerTypeCount: Map<String, Long>? = null,
)

/**
 * Possible statuses for an outbox partition.
 */
enum class OutboxPartitionStatus {
  /** The partition is active and tasks are being dispatched. */
  ACTIVE,

  /** The partition is paused; no tasks will be dispatched until it is resumed. */
  PAUSED,
}
