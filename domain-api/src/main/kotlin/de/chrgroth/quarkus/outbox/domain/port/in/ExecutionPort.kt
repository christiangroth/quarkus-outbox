package de.chrgroth.quarkus.outbox.domain.port.`in`

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority

interface ExecutionPort {

  fun enqueue(
    partition: OutboxPartition,
    event: OutboxEvent,
    payload: String,
    priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
  ): Boolean

  fun activatePartition(partition: OutboxPartition)

  fun archiveFailedTasks(): Long
}
