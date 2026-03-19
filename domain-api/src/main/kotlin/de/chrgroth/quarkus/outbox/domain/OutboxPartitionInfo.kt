package de.chrgroth.quarkus.outbox.domain

import java.time.Instant
import kotlin.reflect.KClass

data class OutboxPartitionInfo(
  val key: String,
  val status: OutboxPartitionStatus,
  val statusReason: String?,
  val pausedUntil: Instant?,
  val eventCount: Long?,
  val eventPerTypeCount: Map<KClass<ApplicationOutboxEvent>, Long>?,
)

enum class OutboxPartitionStatus {
  ACTIVE,
  PAUSED,
}
