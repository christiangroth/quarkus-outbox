package de.chrgroth.outbox

import kotlinx.coroutines.channels.Channel

interface Outbox {

    fun enqueue(
        partition: OutboxPartition,
        event: OutboxEvent,
        payload: String,
        priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
    ): Boolean

    fun processNext(partition: OutboxPartition, dispatch: (OutboxTask) -> OutboxTaskResult): Boolean

    fun signal(partition: OutboxPartition)

    fun getOrCreateChannel(partition: OutboxPartition): Channel<Unit>

    fun resetStaleProcessingTasks()

    fun findPartition(partition: OutboxPartition): OutboxPartitionInfo?

    fun archiveFailedTasks(): Long

    fun activatePartition(partition: OutboxPartition)
}
