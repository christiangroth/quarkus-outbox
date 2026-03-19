package de.chrgroth.quarkus.outbox.domain.port.`in`

import de.chrgroth.quarkus.outbox.domain.OutboxPartition

interface PartitionPort {

  fun activatePartition(partition: OutboxPartition)
}
