package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition

interface OutboxPartitionObserver {
    fun onPartitionPaused(partition: ApplicationOutboxPartition)
    fun onPartitionActivated(partition: ApplicationOutboxPartition)
}
