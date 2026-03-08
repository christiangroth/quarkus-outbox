package de.chrgroth.outbox

interface OutboxTaskDispatcher {
    val partitions: List<OutboxPartition>
    fun dispatch(task: OutboxTask): OutboxTaskResult
}
