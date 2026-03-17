package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.PartitionPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxPartitionObserver
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Any
import jakarta.enterprise.inject.Instance
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

@ApplicationScoped
class PartitionAdapter(
  private val repository: OutboxRepository,
  private val meterRegistry: MeterRegistry,
  @param:Any private val partitionObservers: Instance<OutboxPartitionObserver>,
) : PartitionPort {

  private val partitionStatusGauges = ConcurrentHashMap<String, AtomicInteger>()

  override fun activatePartition(partition: OutboxPartition) {
    repository.activatePartition(partition)
    getOrCreatePartitionStatusGauge(partition).set(1)
    partitionObservers.forEach { it.onPartitionActivated(partition) }
  }

  fun pausePartition(partition: OutboxPartition) {
    getOrCreatePartitionStatusGauge(partition).set(0)
    partitionObservers.forEach { it.onPartitionPaused(partition) }
  }

  private fun getOrCreatePartitionStatusGauge(partition: OutboxPartition): AtomicInteger =
    partitionStatusGauges.getOrPut(partition.key) {
      val initialStatus = repository.findPartition(partition)
        ?.let { if (it.status == OutboxPartitionStatus.ACTIVE.name) 1 else 0 } ?: 1
      AtomicInteger(initialStatus).also { gauge ->
        Gauge.builder("outbox_partition_status", gauge) { it.get().toDouble() }
          .tag("partition", partition.key)
          .description("Outbox partition status: 1=active, 0=paused")
          .register(meterRegistry)
      }
    }
}
