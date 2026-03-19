package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask

interface TaskRepositoryPort {
  fun claim(partition: OutboxPartition): OutboxTask?
  fun delete(task: OutboxTask)
}
