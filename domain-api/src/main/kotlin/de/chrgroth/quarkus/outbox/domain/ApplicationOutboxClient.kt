package de.chrgroth.quarkus.outbox.domain

// Interface for client applications to do something with the outbox.
interface ApplicationOutboxClient {
  fun enqueue(event: ApplicationOutboxEvent)
  fun partitionInfos(): List<OutboxPartitionInfo>
}
