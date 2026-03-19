package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import java.time.Instant

@ApplicationScoped
class OutboxRepositoryAdapter(
  private val taskPort: TaskRepositoryPort,
  private val archivePort: ArchivedTaskRepositoryPort,
  private val partitionPort: PartitionRepositoryPort,
) : OutboxRepository {

  override fun claim(partition: OutboxPartition): OutboxTask? {
    val partitionInfo = findPartition(partition)
    if (partitionInfo != null && partitionInfo.status == OutboxPartitionStatus.PAUSED.name) {
      return null
    }
    return taskPort.claim(partition)
  }

  override fun complete(task: OutboxTask) {
    archivePort.append(task)
    taskPort.delete(task)
  }

  override fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?) {
    if (nextRetryAt == null) {
      archivePort.appendFailed(task, error)
      taskPort.delete(task)
    } else {
      taskPort.scheduleRetry(task, error, nextRetryAt)
    }
  }

  override fun reschedule(task: OutboxTask, nextRetryAt: Instant) = taskPort.reschedule(task, nextRetryAt)

  override fun enqueue(
    partition: OutboxPartition,
    event: OutboxEvent,
    payload: String,
    priority: OutboxTaskPriority,
  ) = taskPort.enqueue(partition, event, payload, priority)

  override fun pausePartition(partition: OutboxPartition, reason: String, pausedUntil: Instant) =
    partitionPort.pause(partition, reason, pausedUntil)

  override fun activatePartition(partition: OutboxPartition) = partitionPort.activate(partition)

  override fun findPartition(partition: OutboxPartition): OutboxPartitionInfo? =
    partitionPort.findPartition(partition.key)

  override fun findOrCreatePartition(partition: OutboxPartition): OutboxPartitionInfo =
    partitionPort.findOrCreate(partition)

  override fun resetStaleProcessingTasks() = taskPort.resetStaleProcessing()

  override fun countByPartition(partition: OutboxPartition) = taskPort.countByPartition(partition)

  override fun archiveFailedTasks(): Long {
    val failedTasks = taskPort.listFailed()
    if (failedTasks.isEmpty()) return 0L
    var count = 0L
    for (task in failedTasks) {
      archivePort.upsertFailed(task)
      taskPort.delete(task)
      count++
    }
    logger.info { "Archived $count failed outbox tasks" }
    return count
  }

  override fun migratePartition(fromKey: String, toPartition: OutboxPartition) =
    taskPort.migratePartition(fromKey, toPartition)

  override fun deleteArchiveEntriesOlderThan(cutoff: Instant) =
    archivePort.deleteEntriesOlderThan(cutoff)

  companion object : KLogging()
}
