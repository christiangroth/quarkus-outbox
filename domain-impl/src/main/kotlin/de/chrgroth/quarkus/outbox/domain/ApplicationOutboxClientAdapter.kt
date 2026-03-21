package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class ApplicationOutboxClientAdapter(
  private val controllerAdapter: OutboxControllerAdapter,
  private val partitionPort: PartitionRepositoryPort,
) : ApplicationOutboxClient {

  override fun enqueue(event: ApplicationOutboxEvent) {
    controllerAdapter.enqueue(event.partition, event, event.serializePayload, event.priority)
  }

  override fun partitionInfos(): List<OutboxPartitionInfo> = partitionPort.findAllPartitions()
}
