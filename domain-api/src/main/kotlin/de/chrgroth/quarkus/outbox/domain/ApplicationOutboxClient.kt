package de.chrgroth.quarkus.outbox.domain

/**
 * Inbound port for applications to interact with the outbox.
 *
 * Use [enqueue] to submit events for asynchronous dispatch, and [partitionInfos]
 * to query the current state of all partitions.
 */
interface ApplicationOutboxClient {

  /**
   * Enqueues the given [event] in the outbox for asynchronous dispatch.
   * If a task with the same deduplication key already exists in the same partition,
   * the event is silently discarded.
   */
  fun enqueue(event: ApplicationOutboxEvent)

  /**
   * Returns the current state of all known outbox partitions.
   */
  fun partitionInfos(): List<OutboxPartitionInfo>
}
