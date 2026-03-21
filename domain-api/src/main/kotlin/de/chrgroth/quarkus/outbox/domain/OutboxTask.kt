package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

/**
 * Represents a single task stored in the outbox.
 *
 * A task is created when an [ApplicationOutboxEvent] is enqueued and carries
 * all information needed to dispatch and track it.
 */
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

/**
 * Dispatch priority for outbox tasks. Higher-priority tasks are processed before normal ones.
 */
enum class OutboxTaskPriority {
    NORMAL,
    HIGH,
}

/**
 * Possible lifecycle states of an outbox task.
 */
enum class OutboxTaskStatus {
    /** Waiting to be claimed for dispatch. */
    PENDING,

    /** Currently being dispatched. */
    PROCESSING,

    /** Successfully dispatched and archived. */
    DONE,

    /** Dispatch failed and the task has been moved to the archive after exhausting retries. */
    FAILED,
}
