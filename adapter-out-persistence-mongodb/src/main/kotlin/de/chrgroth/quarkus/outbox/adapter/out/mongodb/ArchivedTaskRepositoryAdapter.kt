package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskStatus
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.bson.Document
import java.time.Instant

@ApplicationScoped
class ArchivedTaskRepositoryAdapter : ArchivedTaskRepositoryPort {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  @Inject
  lateinit var repository: ArchivedTaskRepository

  override fun append(task: OutboxTask) {
    metricsRecorder.timed("outbox.archive.append") {
      repository.persist(
        buildArchivedTask(task, OutboxTaskStatus.DONE)
      )
    }
  }

  override fun appendFailed(task: OutboxTask, error: String) {
    metricsRecorder.timed("outbox.archive.appendFailed") {
      repository.persist(
        buildArchivedTask(task, OutboxTaskStatus.FAILED)
          .apply {
            attempts = task.attempts + 1
            nextRetryAt = null
            lastError = error
          }
      )
    }
  }

  override fun deleteOlderThan(cutoff: Instant): Long =
    metricsRecorder.timed("outbox.archive.deleteEntriesOlderThan") {
      repository.mongoCollection().deleteMany(
        Filters.lt("completedAt", cutoff),
      )
    }.deletedCount

  override fun deleteAll(): Long =
    metricsRecorder.timed("outbox.archive.deleteAll") {
      repository.mongoCollection().deleteMany(Document())
    }.deletedCount

  override fun count(): Long =
    metricsRecorder.timed("outbox.archive.count") {
      repository.mongoCollection().countDocuments()
    }

  private fun buildArchivedTask(task: OutboxTask, status: OutboxTaskStatus): ArchivedTask =
    Instant.now().let { now ->
      ArchivedTask().apply {
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

