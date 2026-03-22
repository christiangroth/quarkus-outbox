package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
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
    metricsRecorder.timed("outbox.archive.append") {
      persist(
        buildArchivedTask(task, OutboxTaskStatus.DONE)
      )
    }
  }

  override fun appendFailed(task: OutboxTask, error: String) {
    metricsRecorder.timed("outbox.archive.appendFailed") {
      persist(
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
      mongoCollection().deleteMany(
        Filters.lt("completedAt", cutoff),
      )
    }.deletedCount

  override fun count(): Long =
    metricsRecorder.timed("outbox_archive_counting_duration") {
      mongoCollection().countDocuments()
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

