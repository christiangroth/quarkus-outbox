package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.Task
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class TaskRepositoryAdapter : TaskRepositoryPort, PanacheMongoRepositoryBase<Task, String> {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  override fun claim(partition: OutboxPartition): OutboxTask? {
    val now = Instant.now()
    val doc = metricsRecorder.timed("outbox.task.claim") {
      mongoCollection().findOneAndUpdate(
        Filters.and(
          Filters.eq("partition", partition.key),
          Filters.eq("status", OutboxTaskStatus.PENDING.name),
          Filters.or(
            // TODO Simplify
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
          .sort(Sorts.orderBy(Sorts.ascending("priority"), Sorts.ascending("createdAt")))
          .returnDocument(ReturnDocument.AFTER),
      )
    } ?: return null

    return doc.toDomain()
  }

  override fun delete(task: OutboxTask) {
    metricsRecorder.timed("outbox.task.delete") {
      deleteById(task.id)
    }
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
    priority = OutboxTaskPriority.valueOf(priority),
    lastError = lastError,
  )
}
