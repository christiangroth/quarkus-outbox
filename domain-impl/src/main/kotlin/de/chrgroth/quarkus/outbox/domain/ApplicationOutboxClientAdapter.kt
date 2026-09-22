package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import java.time.Instant

@ApplicationScoped
class ApplicationOutboxClientAdapter(
  private val controllerAdapter: OutboxControllerAdapter,
  private val partitionPort: PartitionRepositoryPort,
  private val taskPort: TaskRepositoryPort,
) : ApplicationOutboxClient {

  override fun enqueue(event: ApplicationOutboxEvent, notBefore: Instant?) {
    controllerAdapter.enqueue(event.partition, event, event.serializePayload, event.priority, notBefore)
  }

  override fun cancel(partition: ApplicationOutboxPartition, deduplicationKey: String): Boolean =
    controllerAdapter.cancel(partition, deduplicationKey)

  override fun reschedule(partition: ApplicationOutboxPartition, deduplicationKey: String, notBefore: Instant?): Boolean =
    controllerAdapter.reschedule(partition, deduplicationKey, notBefore)

  override fun partitionInfos(): List<OutboxPartitionInfo> =
    partitionPort.findAllPartitions()

  override fun eventsForPartition(partition: ApplicationOutboxPartition): List<OutboxTask> =
    taskPort.findByPartition(partition)
}
