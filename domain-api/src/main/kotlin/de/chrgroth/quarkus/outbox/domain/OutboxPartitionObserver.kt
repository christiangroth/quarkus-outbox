package de.chrgroth.quarkus.outbox.domain

interface OutboxPartitionObserver {
    fun onPartitionPaused(partition: ApplicationOutboxPartition)
    fun onPartitionActivated(partition: ApplicationOutboxPartition)
}
