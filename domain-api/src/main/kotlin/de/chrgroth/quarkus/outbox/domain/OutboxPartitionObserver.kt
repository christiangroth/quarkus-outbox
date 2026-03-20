package de.chrgroth.quarkus.outbox.domain

/**
 * Observer for outbox partition lifecycle events.
 *
 * Implement and register this interface as a CDI bean to be notified when
 * a partition changes its active/paused state.
 */
interface OutboxPartitionObserver {

  /** Called when the given [partition] transitions from active to paused. */
  fun onPartitionPaused(partition: ApplicationOutboxPartition)

  /** Called when the given [partition] transitions from paused to active. */
  fun onPartitionActivated(partition: ApplicationOutboxPartition)
}
