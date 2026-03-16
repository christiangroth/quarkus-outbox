package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

data class OutboxPartitionInfo(
    val key: String,
    val status: String,
    val statusReason: String?,
    val pausedUntil: Instant?,
)
