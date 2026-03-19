package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.ArchivedTask
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class ArchivedTaskRepositoryAdapter : PanacheMongoRepositoryBase<ArchivedTask, String> {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  fun append(task: OutboxTask) {
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

  fun OutboxTask.toCompletedDocument(): ArchivedTask {
    val now = Instant.now()
    return ArchivedTask().apply {
      id = this@toCompletedDocument.id
      partition = this@toCompletedDocument.partition
      eventType = this@toCompletedDocument.eventType
      deduplicationKey = this@toCompletedDocument.deduplicationKey
      payload = this@toCompletedDocument.payload
      status = OutboxTaskStatus.DONE.name
      attempts = this@toCompletedDocument.attempts
      createdAt = this@toCompletedDocument.createdAt
      updatedAt = now
      nextRetryAt = this@toCompletedDocument.nextRetryAt
      priority = this@toCompletedDocument.priority.name
      lastError = this@toCompletedDocument.lastError
      completedAt = now
    }
  }

  fun ArchivedTask.fail() {

  }
}
