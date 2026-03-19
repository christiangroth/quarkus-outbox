package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.ReplaceOptions
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.ArchivedTask
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class ArchivedTaskRepositoryAdapter : ArchivedTaskRepositoryPort, PanacheMongoRepositoryBase<ArchivedTask, String> {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  override fun append(task: OutboxTask) {
    val now = Instant.now()
    val archiveDoc = ArchivedTask().apply {
      id = task.id
      partition = task.partition
      eventType = task.eventType
      deduplicationKey = task.deduplicationKey
      payload = task.payload
      status = OutboxTaskStatus.DONE.name
      attempts = task.attempts
      createdAt = task.createdAt
      updatedAt = now
      nextRetryAt = task.nextRetryAt
      priority = task.priority.name
      lastError = task.lastError
      completedAt = now
    }
    metricsRecorder.timed("outbox.archive.append") {
      persist(archiveDoc)
    }
  }

  override fun appendFailed(task: OutboxTask, error: String) {
    val now = Instant.now()
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
      nextRetryAt = null
      priority = task.priority.name
      lastError = error
      completedAt = now
    }
    metricsRecorder.timed("outbox.archive.appendFailed") {
      persist(archiveDoc)
    }
  }

  override fun upsertFailed(task: OutboxTask) {
    val now = Instant.now()
    val archiveDoc = ArchivedTask().apply {
      id = task.id
      partition = task.partition
      eventType = task.eventType
      deduplicationKey = task.deduplicationKey
      payload = task.payload
      status = OutboxTaskStatus.FAILED.name
      attempts = task.attempts
      createdAt = task.createdAt
      updatedAt = now
      nextRetryAt = null
      priority = task.priority.name
      lastError = task.lastError
      completedAt = now
    }
    metricsRecorder.timed("outbox.archive.upsertFailed") {
      mongoCollection().replaceOne(
        Filters.eq("_id", task.id),
        archiveDoc,
        ReplaceOptions().upsert(true),
      )
    }
  }

  override fun deleteEntriesOlderThan(cutoff: Instant): Long {
    val result = metricsRecorder.timed("outbox.archive.deleteEntriesOlderThan") {
      mongoCollection().deleteMany(
        Filters.lt("completedAt", cutoff),
      )
    }
    return result.deletedCount
  }
}

