package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxTask
import java.time.Instant

interface ArchivedTaskRepositoryPort {
  fun append(task: OutboxTask)
  fun appendFailed(task: OutboxTask, error: String)
  fun deleteOlderThan(cutoff: Instant): Long
}
