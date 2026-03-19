package de.chrgroth.quarkus.outbox.domain.port.`in`

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxEvent
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority

interface OutboxControllerPort {

  fun enqueue(
    partition: ApplicationOutboxPartition,
    event: ApplicationOutboxEvent,
    payload: String,
    priority: OutboxTaskPriority = OutboxTaskPriority.NORMAL,
  ): Boolean

  fun activatePartition(partition: ApplicationOutboxPartition)
}
