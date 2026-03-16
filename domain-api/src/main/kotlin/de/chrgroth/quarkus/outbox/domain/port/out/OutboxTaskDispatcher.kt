package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTask
import de.chrgroth.quarkus.outbox.domain.OutboxTaskResult

interface OutboxTaskDispatcher {
    val partitions: List<OutboxPartition>
    fun dispatch(task: OutboxTask): OutboxTaskResult
}
