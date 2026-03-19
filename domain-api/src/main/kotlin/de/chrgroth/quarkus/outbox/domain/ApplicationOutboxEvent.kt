package de.chrgroth.quarkus.outbox.domain

interface ApplicationOutboxEvent {
  val key: String
  val partition: ApplicationOutboxPartition
  val priority: OutboxTaskPriority
  val deduplicationKey: String
  val serializePayload: String
}
