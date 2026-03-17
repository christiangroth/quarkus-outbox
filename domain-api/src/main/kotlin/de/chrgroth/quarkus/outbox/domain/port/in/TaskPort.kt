package de.chrgroth.quarkus.outbox.domain.port.`in`

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority

interface TaskPort {

  fun enqueue(
    partition: OutboxPartition,
    event: OutboxEvent,
    payload: String,
    priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
  ): Boolean
}
