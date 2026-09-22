package de.chrgroth.quarkus.outbox.domain

import java.time.Instant

/**
 * Inbound port for applications to interact with the outbox.
 *
 * Use [enqueue] to submit events for asynchronous dispatch, [partitionInfos]
 * to query the current state of all partitions, and [eventsForPartition] to
 * retrieve pending events for a specific partition in execution order.
 *
 * [cancel] and [reschedule] allow superseding an already-enqueued, not-yet-dispatched
 * task by its [ApplicationOutboxEvent.deduplicationKey] – typically used together with a
 * delayed [enqueue] to implement cron-style recurring dispatch (see [notBefore] on [enqueue]).
 */
interface ApplicationOutboxClient {

  /**
   * Enqueues the given [event] in the outbox for asynchronous dispatch.
   * If a task with the same deduplication key already exists in the same partition,
   * the event is silently discarded.
   *
   * When [notBefore] is given, the task is only eligible for pickup at or after that
   * instant, allowing delayed/scheduled dispatch. Leave it `null` (the default) for
   * immediate dispatch, matching the previous behaviour.
   *
   * A recurring task can be implemented by having the [ApplicationOutboxDispatcher.dispatch]
   * handler call [enqueue] again for the next occurrence (with a new [notBefore]) as part of
   * successfully completing the current one.
   */
  fun enqueue(event: ApplicationOutboxEvent, notBefore: Instant? = null)

  /**
   * Cancels the pending task in [partition] identified by [deduplicationKey], if one exists.
   * Returns `true` if a pending task was found and cancelled, `false` if no such task exists
   * or it is no longer pending (e.g. already dispatched or currently being dispatched).
   *
   * Useful to supersede a previously enqueued delayed/recurring task, e.g. when the schedule
   * it was derived from (a cron expression, a due date, ...) changes or is removed.
   */
  fun cancel(partition: ApplicationOutboxPartition, deduplicationKey: String): Boolean

  /**
   * Reschedules the pending task in [partition] identified by [deduplicationKey] to the new
   * [notBefore] instant (or immediate eligibility when `null`), if one exists.
   * Returns `true` if a pending task was found and rescheduled, `false` if no such task exists
   * or it is no longer pending.
   *
   * Useful when the "next due time" for a recurring/delayed task changes without needing to
   * cancel and re-enqueue.
   */
  fun reschedule(partition: ApplicationOutboxPartition, deduplicationKey: String, notBefore: Instant?): Boolean

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
