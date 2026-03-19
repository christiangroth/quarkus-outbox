package de.chrgroth.quarkus.outbox.domain

// TODO define CDI events and their payloads
// - activated: partition
// - paused: partition, reason
// - ...
interface OutboxPartitionObserver {
  fun onPartitionPaused(partition: ApplicationOutboxPartition)
  fun onPartitionActivated(partition: ApplicationOutboxPartition)
}
