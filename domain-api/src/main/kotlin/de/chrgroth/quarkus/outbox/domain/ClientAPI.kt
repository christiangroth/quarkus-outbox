package de.chrgroth.quarkus.outbox.domain

// model
//sealed interface DomainOutboxEvent : OutboxEvent {
//  // TODO move to outbox
//  val partition: DomainOutboxPartition
//
//  // TODO move to outbox
//  val priority: OutboxTaskPriority get() = OutboxTaskPriority.NORMAL
//
//  // TODO move to outbox
//  fun toPayload(): String


// in
//     override fun dispatch(task: OutboxTask): OutboxTaskResult {
// OutboxPartitionObserver
// - onPartitionPaused -> reason

// out
//         val inserted = outbox.enqueue(event.partition, event, event.toPayload(), event.priority)

//override fun getPartitionStats(): List<OutboxPartitionStats> =
//  DomainOutboxPartition.all.map { partition ->
//    val info = repository.findPartition(partition)
//    OutboxPartitionStats(
//      name = partition.key,
//      status = info?.status ?: OutboxPartitionStatus.ACTIVE.name,
//      documentCount = repository.countByPartition(partition),
//      blockedUntil = info?.pausedUntil,
//      eventTypeCounts = queryEventTypeCounts(partition.key),
//    )
//  }
