package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Aggregates
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
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import mu.KLogging
import java.time.Instant
import java.util.UUID

@ApplicationScoped
class TaskRepositoryAdapter : TaskRepositoryPort {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  @Inject
  lateinit var repository: TaskRepository

  override fun claim(partition: ApplicationOutboxPartition): OutboxTask? {
    val now = Instant.now()
    return metricsRecorder.timed("outbox.task.claim") {
      repository.mongoCollection().findOneAndUpdate(
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
          .sort(EXECUTION_ORDER_SORT)
          .returnDocument(ReturnDocument.AFTER),
      )
    }?.toDomain()
  }

  override fun delete(task: OutboxTask) {
    metricsRecorder.timed("outbox.task.delete") {
      repository.deleteById(task.id)
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
      repository.mongoCollection().find(
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
      repository.persist(Task().apply {
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
      repository.mongoCollection().updateOne(
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
      repository.mongoCollection().updateOne(
        Filters.eq("_id", task.id),
        Updates.combine(
          Updates.set("status", OutboxTaskStatus.PENDING.name),
          Updates.set("updatedAt", now),
          Updates.set("nextRetryAt", nextRetryAt),
        ),
      )
    }
  }

  override fun findEarliestPendingRetryAt(partition: ApplicationOutboxPartition): Instant? =
    metricsRecorder.timed("outbox.task.findEarliestPendingRetryAt") {
      repository.mongoCollection().find(
        Filters.and(
          Filters.eq("partition", partition.key),
          Filters.eq("status", OutboxTaskStatus.PENDING.name),
          Filters.ne("nextRetryAt", null),
        ),
      ).sort(Sorts.ascending("nextRetryAt")).first()
    }?.nextRetryAt

  override fun resetStaleProcessing() {
    val now = Instant.now()
    val result = metricsRecorder.timed("outbox.task.resetStaleProcessing") {
      repository.mongoCollection().updateMany(
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
      repository.count("partition = ?1", partition.key)
    }

  override fun countByEventType(partitionKey: String): Map<String, Long> =
    metricsRecorder.timed("outbox.task.countByEventType") {
      repository.mongoCollection().aggregate(
        listOf(
          Aggregates.match(Filters.eq("partition", partitionKey)),
          Aggregates.group("\$eventType", Accumulators.sum("count", 1L)),
        ),
        org.bson.Document::class.java,
      ).associate { it.getString("_id") to it.getLong("count") }
    }

  override fun findByPartition(partition: ApplicationOutboxPartition): List<OutboxTask> =
    metricsRecorder.timed("outbox.task.findByPartition") {
      repository.mongoCollection()
        .find(Filters.eq("partition", partition.key))
        .sort(EXECUTION_ORDER_SORT)
        .map { it.toDomain() }
        .toList()
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
    priority = runCatching { OutboxEventPriority.valueOf(priority) }.getOrDefault(OutboxEventPriority.MEDIUM),
    lastError = lastError,
  )

  companion object : KLogging() {
    val EXECUTION_ORDER_SORT = Sorts.orderBy(Sorts.ascending("priorityOrder"), Sorts.ascending("createdAt"))
  }
}
