package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

data class OutboxPartitionInfo(
    val key: String,
    val status: OutboxPartitionStatus,
    val statusReason: String?,
    val pausedUntil: Instant?,
)

enum class OutboxPartitionStatus {
    ACTIVE,
    PAUSED,
}
