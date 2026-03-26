package de.chrgroth.quarkus.outbox.domain

/**
 * Inbound port for applications to interact with the outbox.
 *
 * Use [enqueue] to submit events for asynchronous dispatch, [partitionInfos]
 * to query the current state of all partitions, and [eventsForPartition] to
 * retrieve pending events for a specific partition in execution order.
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

  /**
   * Returns all outbox tasks for [partition], ordered by execution priority
   * (highest priority first, then by enqueue time ascending).
   * Includes tasks in any status (e.g. [OutboxTaskStatus.PENDING], [OutboxTaskStatus.PROCESSING]).
   */
  fun eventsForPartition(partition: ApplicationOutboxPartition): List<OutboxTask>
}
