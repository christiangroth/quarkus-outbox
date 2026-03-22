package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxEvent
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxEventPriority
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import mu.KLogging
import java.time.Instant
import java.util.UUID

@ApplicationScoped
class TaskRepositoryAdapter : TaskRepositoryPort, PanacheMongoRepositoryBase<Task, String> {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  override fun claim(partition: ApplicationOutboxPartition): OutboxTask? {
    val now = Instant.now()
    return metricsRecorder.timed("outbox.task.claim") {
      mongoCollection().findOneAndUpdate(
        Filters.and(
          Filters.eq("partition", partition.key),
          Filters.eq("status", OutboxTaskStatus.PENDING.name),
          Filters.or(
            Filters.exists("nextRetryAt", false),
            Filters.eq("nextRetryAt", null),
            Filters.lte("nextRetryAt", now),
          ),
        ),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PROCESSING.name),
          Updates.set("updatedAt", now),
        ),
        FindOneAndUpdateOptions()
          .sort(Sorts.orderBy(Sorts.ascending("priorityOrder"), Sorts.ascending("createdAt")))
          .returnDocument(ReturnDocument.AFTER),
      )
    }?.toDomain()
  }

  override fun delete(task: OutboxTask) {
    metricsRecorder.timed("outbox.task.delete") {
      deleteById(task.id)
    }
  }

  override fun enqueue(
    partition: ApplicationOutboxPartition,
    event: ApplicationOutboxEvent,
    payload: String,
    priority: OutboxEventPriority,
  ): Boolean {
    val deduplicationKey = event.deduplicationKey
    val existing = metricsRecorder.timed("outbox.task.dedupCheck") {
      mongoCollection().find(
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
    metricsRecorder.timed("outbox.task.insert") {
      persist(Task().apply {
        id = UUID.randomUUID().toString()
        this.partition = partition.key
        eventType = event.key
        this.deduplicationKey = deduplicationKey
        this.payload = payload
        status = OutboxTaskStatus.PENDING.name
        attempts = 0
        createdAt = now
        updatedAt = now
        nextRetryAt = null
        this.priority = priority.name
        this.priorityOrder = priority.sortOrder
        lastError = null
      })
    }

    return true
  }

  override fun scheduleRetry(task: OutboxTask, error: String, nextRetryAt: Instant) {
    val now = Instant.now()
    metricsRecorder.timed("outbox.task.scheduleRetry") {
      mongoCollection().updateOne(
        Filters.eq("_id", task.id),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PENDING.name),
          Updates.inc("attempts", 1),
          Updates.set("updatedAt", now),
          Updates.set("lastError", error),
          Updates.set("nextRetryAt", nextRetryAt),
        ),
      )
    }
  }

  override fun reschedule(task: OutboxTask, nextRetryAt: Instant) {
    val now = Instant.now()
    metricsRecorder.timed("outbox.task.reschedule") {
      mongoCollection().updateOne(
        Filters.eq("_id", task.id),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PENDING.name),
          Updates.set("updatedAt", now),
          Updates.set("nextRetryAt", nextRetryAt),
        ),
      )
    }
  }

  override fun resetStaleProcessing() {
    val now = Instant.now()
    val result = metricsRecorder.timed("outbox.task.resetStaleProcessing") {
      mongoCollection().updateMany(
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

  override fun countByPartition(partition: ApplicationOutboxPartition): Long =
    metricsRecorder.timed("outbox.task.countByPartition") {
      count("partition = ?1", partition.key)
    }

  private fun Task.toDomain() = OutboxTask(
    id = id,
    partition = partition,
    eventType = eventType,
    payload = payload,
    deduplicationKey = deduplicationKey,
    status = OutboxTaskStatus.valueOf(status),
    attempts = attempts,
    createdAt = createdAt,
    updatedAt = updatedAt,
    nextRetryAt = nextRetryAt,
    priority = if (priority == "NORMAL") OutboxEventPriority.MEDIUM else OutboxEventPriority.valueOf(priority),
    lastError = lastError,
  )

  companion object : KLogging()
}
