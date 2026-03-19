package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import java.time.Instant

@ApplicationScoped
class ArchiverAdapter(
  private val taskPort: TaskRepositoryPort,
  private val archivePort: ArchivedTaskRepositoryPort,
) : ArchiverPort {

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

  override fun deleteEntriesOlderThan(cutoff: Instant): Long =
    archivePort.deleteEntriesOlderThan(cutoff)

  companion object : KLogging()
}
