package de.chrgroth.quarkus.outbox.domain

interface ApplicationPort {
  fun getAllPartitions(): List<OutboxPartition>
  fun dispatch(task: OutboxTask): OutboxTaskResult
}
