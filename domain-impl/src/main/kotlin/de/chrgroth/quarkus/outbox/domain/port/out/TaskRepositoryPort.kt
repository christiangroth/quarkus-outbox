package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import java.time.Instant

interface TaskRepositoryPort {
  fun claim(partition: OutboxPartition): OutboxTask?
  fun delete(task: OutboxTask)
  fun enqueue(partition: OutboxPartition, event: OutboxEvent, payload: String, priority: OutboxTaskPriority): Boolean
  fun scheduleRetry(task: OutboxTask, error: String, nextRetryAt: Instant)
  fun reschedule(task: OutboxTask, nextRetryAt: Instant)
  fun resetStaleProcessing()
  fun countByPartition(partition: OutboxPartition): Long
  fun migratePartition(fromKey: String, toPartition: OutboxPartition): Long
  fun listFailed(): List<OutboxTask>
}
