package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import java.time.Instant

interface PartitionRepositoryPort {
  fun findPartition(partitionKey: String): OutboxPartitionInfo?
  fun findOrCreate(partition: ApplicationOutboxPartition): OutboxPartitionInfo
  fun pause(partition: ApplicationOutboxPartition, reason: String, pausedUntil: Instant)
  fun activate(partition: ApplicationOutboxPartition)
}
