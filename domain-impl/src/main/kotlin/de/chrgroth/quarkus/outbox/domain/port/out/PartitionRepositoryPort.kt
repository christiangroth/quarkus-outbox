package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import java.time.Instant

interface PartitionRepositoryPort {
  fun findPartition(partitionKey: String): OutboxPartitionInfo?
  fun findOrCreate(partition: OutboxPartition): OutboxPartitionInfo
  fun pause(partition: OutboxPartition, reason: String, pausedUntil: Instant)
  fun activate(partition: OutboxPartition)
}
