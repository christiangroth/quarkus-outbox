package de.chrgroth.outbox

import java.time.Instant

interface OutboxRepository {
    fun claim(partition: OutboxPartition): OutboxTask?
    fun complete(task: OutboxTask)
    fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?)
    fun reschedule(task: OutboxTask, nextRetryAt: Instant)
    fun enqueue(
        partition: OutboxPartition,
        event: OutboxEvent,
        payload: String,
        priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
    ): Boolean
    fun pausePartition(partition: OutboxPartition, reason: String, pausedUntil: Instant)
    fun activatePartition(partition: OutboxPartition)
    fun findPartition(partition: OutboxPartition): OutboxPartitionInfo?
    fun resetStaleProcessingTasks()
    fun countByPartition(partition: OutboxPartition): Long
    fun archiveFailedTasks(): Long
    fun migratePartition(fromKey: String, toPartition: OutboxPartition): Long
}
