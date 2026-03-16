package de.chrgroth.quarkus.outbox.domain.port.`in`

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority

interface Outbox {

    fun enqueue(
        partition: OutboxPartition,
        event: OutboxEvent,
        payload: String,
        priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
    ): Boolean

    fun findPartition(partition: OutboxPartition): OutboxPartitionInfo?

    fun activatePartition(partition: OutboxPartition)

    fun archiveFailedTasks(): Long
}
