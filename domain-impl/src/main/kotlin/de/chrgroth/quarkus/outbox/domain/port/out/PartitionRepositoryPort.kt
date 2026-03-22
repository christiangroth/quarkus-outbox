package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import java.time.Instant

interface PartitionRepositoryPort {
  fun findOrCreate(partition: ApplicationOutboxPartition): OutboxPartitionInfo
  fun findAllPartitions(): List<OutboxPartitionInfo>
  fun pause(partition: ApplicationOutboxPartition, reason: String?, pausedUntil: Instant?)
  fun resume(partition: ApplicationOutboxPartition)
}
