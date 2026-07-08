package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.PartitionCountReconciliationPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging

@ApplicationScoped
class PartitionCountReconciliationAdapter(
  private val partitionPort: PartitionRepositoryPort,
  private val taskPort: TaskRepositoryPort,
) : PartitionCountReconciliationPort {

  override fun reconcileEventTypeCounts(): Long =
    partitionPort.findAllPartitions().count { info ->
      val partition = object : ApplicationOutboxPartition {
        override val key = info.key
      }
      val actual = taskPort.countByEventType(partition.key)
      val persisted = info.eventPerTypeCount.orEmpty()
      val drifted = actual != persisted
      if (drifted) {
        partitionPort.replaceEventTypeCounts(partition, actual)
        logger.info { "Corrected drifted event type counts for partition '${info.key}': $persisted -> $actual" }
      }
      drifted
    }.toLong()

  companion object : KLogging()
}
