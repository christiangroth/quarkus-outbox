package de.chrgroth.outbox

import java.time.Instant

data class OutboxTask(
    val id: String,
    val partition: String,
    val eventType: String,
    val payload: String,
    val deduplicationKey: String,
    val status: OutboxTaskStatus,
    val attempts: Int,
    val createdAt: Instant,
    val updatedAt: Instant,
    val nextRetryAt: Instant?,
    val priority: OutboxTaskPriority,
    val lastError: String?,
)
