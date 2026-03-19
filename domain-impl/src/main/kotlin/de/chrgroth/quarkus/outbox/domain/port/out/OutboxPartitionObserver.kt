package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition

interface OutboxPartitionObserver {
    fun onPartitionPaused(partition: OutboxPartition)
    fun onPartitionActivated(partition: OutboxPartition)
}
