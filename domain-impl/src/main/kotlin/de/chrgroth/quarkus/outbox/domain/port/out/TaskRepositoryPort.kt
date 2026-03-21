package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxEvent
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import java.time.Instant

interface TaskRepositoryPort {
  fun claim(partition: ApplicationOutboxPartition): OutboxTask?
  fun delete(task: OutboxTask)
  fun enqueue(partition: ApplicationOutboxPartition, event: ApplicationOutboxEvent, payload: String, priority: OutboxTaskPriority): Boolean
  fun scheduleRetry(task: OutboxTask, error: String, nextRetryAt: Instant)
  fun reschedule(task: OutboxTask, nextRetryAt: Instant)
  fun resetStaleProcessing()
  fun countByPartition(partition: ApplicationOutboxPartition): Long
}
