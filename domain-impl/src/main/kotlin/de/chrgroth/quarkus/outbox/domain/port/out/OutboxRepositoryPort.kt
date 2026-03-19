package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxTask
import java.time.Instant

interface OutboxRepositoryPort {
  fun complete(task: OutboxTask)
  fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?)
}
