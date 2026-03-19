package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.ArchivedTask
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.Partition
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.Task
import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import mu.KLogging
import java.time.Instant
import java.util.UUID

// TODO move remaining to domain-impl
@ApplicationScoped
class MongoOutboxRepository : OutboxRepository {

  @Inject
  lateinit var tasks: TaskRepositoryPort

  @Inject
  lateinit var partitions: PartitionRepository

  @Inject
  lateinit var archive: ArchivedTaskRepositoryAdapter

  override fun claim(partition: OutboxPartition): OutboxTask? {
    val partitionDoc = findPartition(partition)
    if (partitionDoc != null && partitionDoc.status == OutboxPartitionStatus.PAUSED.name) {
      return null
    }

    return tasks.claim(partition)
  }

  override fun complete(task: OutboxTask) {
    archive.append(task)
    tasks.delete(task)
  }

  override fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?) {
    val now = Instant.now()
    if (nextRetryAt == null) {
      val archiveDoc = ArchivedTask().apply {
        id = task.id
        partition = task.partition
        eventType = task.eventType
        deduplicationKey = task.deduplicationKey
        payload = task.payload
        status = OutboxTaskStatus.FAILED.name
        attempts = task.attempts + 1
        createdAt = task.createdAt
        updatedAt = now
        this.nextRetryAt = null
        priority = task.priority.name
        lastError = error
        completedAt = now
      }
      metricsRecorder.timed("outbox.fail.archive") {
        archive.persist(archiveDoc)
      }
      metricsRecorder.timed("outbox.fail.delete") {
        tasks.deleteById(task.id)
      }
    } else {
      val updates = mutableListOf(
        Updates.set("status", OutboxTaskStatus.PENDING.name),
        Updates.inc("attempts", 1),
        Updates.set("updatedAt", now),
        Updates.set("lastError", error),
        Updates.set("nextRetryAt", nextRetryAt),
      )
      metricsRecorder.timed("outbox.fail") {
        tasks.mongoCollection().updateOne(
          Filters.eq("_id", task.id),
          Updates.combine(updates),
        )
      }
    }
  }

  override fun reschedule(task: OutboxTask, nextRetryAt: Instant) {
    val now = Instant.now()
    metricsRecorder.timed("outbox.reschedule") {
      tasks.mongoCollection().updateOne(
        Filters.eq("_id", task.id),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PENDING.name),
          Updates.set("updatedAt", now),
          Updates.set("nextRetryAt", nextRetryAt),
        ),
      )
    }
  }

  override fun enqueue(
    partition: OutboxPartition,
    event: OutboxEvent,
    payload: String,
    priority: OutboxTaskPriority,
  ): Boolean {
    val deduplicationKey = event.deduplicationKey()
    val existing = metricsRecorder.timed("outbox.enqueue.dedupCheck") {
      tasks.mongoCollection().find(
        Filters.and(
          Filters.eq("partition", partition.key),
          Filters.eq("deduplicationKey", deduplicationKey),
          Filters.`in`("status", OutboxTaskStatus.PENDING.name, OutboxTaskStatus.PROCESSING.name),
        ),
      ).first()
    }

    if (existing != null) {
      logger.debug { "Skipping duplicate outbox task: partition=${partition.key}, deduplicationKey=$deduplicationKey" }
      return false
    }

    val now = Instant.now()
    val doc = Task().apply {
      id = UUID.randomUUID().toString()
      this.partition = partition.key
      this.eventType = event.key
      this.deduplicationKey = deduplicationKey
      this.payload = payload
      status = OutboxTaskStatus.PENDING.name
      attempts = 0
      createdAt = now
      updatedAt = now
      nextRetryAt = null
      this.priority = priority.name
      lastError = null
    }
    metricsRecorder.timed("outbox.enqueue.insert") {
      tasks.persist(doc)
    }
    return true
  }

  override fun pausePartition(partition: OutboxPartition, reason: String, pausedUntil: Instant) {
    metricsRecorder.timed("outbox.pausePartition") {
      partitions.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(
          Updates.set("status", OutboxPartitionStatus.PAUSED.name),
          Updates.set("statusReason", reason),
          Updates.set("pausedUntil", pausedUntil),
        ),
        FindOneAndUpdateOptions().upsert(true),
      )
    }
  }

  override fun activatePartition(partition: OutboxPartition) {
    metricsRecorder.timed("outbox.activatePartition") {
      partitions.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(
          Updates.set("status", OutboxPartitionStatus.ACTIVE.name),
          Updates.unset("statusReason"),
          Updates.unset("pausedUntil"),
        ),
        FindOneAndUpdateOptions().upsert(true),
      )
    }
  }

  override fun findPartition(partition: OutboxPartition): OutboxPartitionInfo? =
    metricsRecorder.timed("outbox.findPartition") {
      partitions.findById(partition.key)?.toInfo()
    }

  override fun findOrCreatePartition(partition: OutboxPartition): OutboxPartitionInfo {
    val doc = metricsRecorder.timed("outbox.findOrCreatePartition") {
      partitions.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
    return doc!!.toInfo()
  }

  private fun Partition.toInfo() = OutboxPartitionInfo(
    key = partitionKey,
    status = status,
    statusReason = statusReason,
    pausedUntil = pausedUntil,
  )

  /** Resets all PROCESSING tasks back to PENDING. Should be called at application startup to recover tasks that were interrupted mid-processing. */
  override fun resetStaleProcessingTasks() {
    val now = Instant.now()
    val result = metricsRecorder.timed("outbox.resetStaleProcessingTasks") {
      tasks.mongoCollection().updateMany(
        Filters.eq("status", OutboxTaskStatus.PROCESSING.name),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PENDING.name),
          Updates.set("nextRetryAt", now),
          Updates.set("updatedAt", now),
        ),
      )
    }
    if (result.modifiedCount > 0) {
      logger.info { "Reset ${result.modifiedCount} stale PROCESSING tasks back to PENDING" }
    }
  }

  override fun countByPartition(partition: OutboxPartition): Long =
    metricsRecorder.timed("outbox.countByPartition") {
      tasks.count("partition = ?1", partition.key)
    }

  override fun migratePartition(fromKey: String, toPartition: OutboxPartition): Long {
    val now = Instant.now()
    val result = metricsRecorder.timed("outbox.migratePartition") {
      tasks.mongoCollection().updateMany(
        Filters.eq("partition", fromKey),
        Updates.combine(
          Updates.set("partition", toPartition.key),
          Updates.set("updatedAt", now),
        ),
      )
    }
    return result.modifiedCount
  }

  override fun deleteArchiveEntriesOlderThan(cutoff: Instant): Long {
    val result = metricsRecorder.timed("outbox_archive.deleteEntriesOlderThan") {
      archive.mongoCollection().deleteMany(
        Filters.lt("completedAt", cutoff),
      )
    }
    return result.deletedCount
  }

  override fun archiveFailedTasks(): Long {
    val now = Instant.now()
    val failedDocs = metricsRecorder.timed("outbox.archiveFailedTasks.find") {
      tasks.list("status = ?1", OutboxTaskStatus.FAILED.name)
    }
    if (failedDocs.isEmpty()) return 0L
    var count = 0L
    for (doc in failedDocs) {
      val archiveDoc = ArchivedTask().apply {
        id = doc.id
        partition = doc.partition
        eventType = doc.eventType
        deduplicationKey = doc.deduplicationKey
        payload = doc.payload
        status = OutboxTaskStatus.FAILED.name
        attempts = doc.attempts
        createdAt = doc.createdAt
        updatedAt = now
        nextRetryAt = null
        priority = doc.priority
        lastError = doc.lastError
        completedAt = now
      }
      metricsRecorder.timed("outbox.archiveFailedTasks.archive") {
        archive.mongoCollection().replaceOne(
          Filters.eq("_id", doc.id),
          archiveDoc,
          ReplaceOptions().upsert(true),
        )
      }
      metricsRecorder.timed("outbox.archiveFailedTasks.delete") {
        tasks.deleteById(doc.id)
      }
      count++
    }
    logger.info { "Archived $count failed outbox tasks" }
    return count
  }

  companion object : KLogging()
}
