package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxEvent
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxEventPriority
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import java.time.Instant

interface TaskRepositoryPort {
  fun claim(partition: ApplicationOutboxPartition): OutboxTask?
  fun delete(task: OutboxTask)
  fun enqueue(
    partition: ApplicationOutboxPartition,
    event: ApplicationOutboxEvent,
    payload: String,
    priority: OutboxEventPriority,
    notBefore: Instant? = null,
  ): Boolean
  fun scheduleRetry(task: OutboxTask, error: String, nextRetryAt: Instant)
  fun reschedule(task: OutboxTask, nextRetryAt: Instant)
  fun findEarliestPendingRetryAt(partition: ApplicationOutboxPartition): Instant?
  fun findEarliestPendingNotBeforeAt(partition: ApplicationOutboxPartition): Instant?
  fun resetStaleProcessing()
  fun countByPartition(partition: ApplicationOutboxPartition): Long
  fun countByEventType(partitionKey: String): Map<String, Long>
  fun findByPartition(partition: ApplicationOutboxPartition): List<OutboxTask>

  /**
   * Cancels the pending task identified by [partition] and [deduplicationKey], returning the
   * cancelled task, or `null` if no such pending task exists.
   */
  fun cancelByDeduplicationKey(partition: ApplicationOutboxPartition, deduplicationKey: String): OutboxTask?

  /**
   * Reschedules the pending task identified by [partition] and [deduplicationKey] to [notBefore],
   * returning the updated task, or `null` if no such pending task exists.
   */
  fun rescheduleByDeduplicationKey(partition: ApplicationOutboxPartition, deduplicationKey: String, notBefore: Instant?): OutboxTask?
}
