package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import jakarta.enterprise.inject.Instance
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PartitionAdapterTests {

  private val repository: OutboxRepository = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val partitionObserver: OutboxPartitionObserver = mockk(relaxed = true)

  @Suppress("UNCHECKED_CAST")
  private val partitionObservers: Instance<OutboxPartitionObserver> = mockk<Instance<OutboxPartitionObserver>>().also {
    every { it.iterator() } answers { mutableListOf(partitionObserver).iterator() }
  }

  private val adapter = PartitionAdapter(repository, meterRegistry, partitionObservers)

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  @Test
  fun `activatePartition calls repository activates gauge and notifies observers`() {
    every { repository.activatePartition(partition) } just runs
    every { repository.findPartition(partition) } returns null

    adapter.activatePartition(partition)

    verify { repository.activatePartition(partition) }
    verify { partitionObserver.onPartitionActivated(partition) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `activatePartition initialises gauge from persisted status when already paused`() {
    every { repository.activatePartition(partition) } just runs
    every { repository.findPartition(partition) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "rate_limited",
      pausedUntil = null,
    )

    adapter.activatePartition(partition)

    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(1.0)
  }

  @Test
  fun `pausePartition sets gauge to zero and notifies observers`() {
    every { repository.findPartition(partition) } returns null

    adapter.pausePartition(partition)

    verify { partitionObserver.onPartitionPaused(partition) }
    assertThat(meterRegistry.find("outbox_partition_status").tag("partition", partition.key).gauge()?.value()).isEqualTo(0.0)
  }
}
