package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.ReplaceOptions
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
    val archiveDoc = buildArchivedTask(task, OutboxTaskStatus.DONE)
    metricsRecorder.timed("outbox.archive.append") {
      persist(archiveDoc)
    }
  }

  override fun appendFailed(task: OutboxTask, error: String) {
    val archiveDoc = buildArchivedTask(task, OutboxTaskStatus.FAILED).apply {
      attempts = task.attempts + 1
      nextRetryAt = null
      lastError = error
    }
    metricsRecorder.timed("outbox.archive.appendFailed") {
      persist(archiveDoc)
    }
  }

  override fun upsertFailed(task: OutboxTask) {
    val archiveDoc = buildArchivedTask(task, OutboxTaskStatus.FAILED).apply {
      nextRetryAt = null
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

  private fun buildArchivedTask(task: OutboxTask, status: OutboxTaskStatus): ArchivedTask {
    val now = Instant.now()
    return ArchivedTask().apply {
      id = task.id
      partition = task.partition
      eventType = task.eventType
      deduplicationKey = task.deduplicationKey
      payload = task.payload
      this.status = status.name
      attempts = task.attempts
      createdAt = task.createdAt
      updatedAt = now
      nextRetryAt = task.nextRetryAt
      priority = task.priority.name
      lastError = task.lastError
      completedAt = now
    }
  }
}

