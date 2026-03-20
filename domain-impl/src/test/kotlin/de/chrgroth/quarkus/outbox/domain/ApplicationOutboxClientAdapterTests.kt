package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ApplicationOutboxClientAdapterTests {

  private val controllerAdapter: OutboxControllerAdapter = mockk(relaxed = true)
  private val partitionPort: PartitionRepositoryPort = mockk()
  private val clientAdapter = ApplicationOutboxClientAdapter(controllerAdapter, partitionPort)

  private val partition = object : ApplicationOutboxPartition {
    override val key = "test-partition"
  }

  private val event = object : ApplicationOutboxEvent {
    override val key = "TEST_EVENT"
    override val partition = this@ApplicationOutboxClientAdapterTests.partition
    override val priority = OutboxTaskPriority.NORMAL
    override val deduplicationKey = "dedup-1"
    override val serializePayload = """{"data":"value"}"""
  }

  @Test
  fun `enqueue delegates to controller adapter with all event fields`() {
    every { controllerAdapter.enqueue(partition, event, event.serializePayload, event.priority) } returns true

    clientAdapter.enqueue(event)

    verify { controllerAdapter.enqueue(partition, event, event.serializePayload, event.priority) }
  }

  @Test
  fun `enqueue delegates with HIGH priority when event specifies it`() {
    val highPriorityEvent = object : ApplicationOutboxEvent {
      override val key = "HIGH_EVENT"
      override val partition = this@ApplicationOutboxClientAdapterTests.partition
      override val priority = OutboxTaskPriority.HIGH
      override val deduplicationKey = "dedup-high"
      override val serializePayload = "{}"
    }
    every { controllerAdapter.enqueue(partition, highPriorityEvent, "{}", OutboxTaskPriority.HIGH) } returns true

    clientAdapter.enqueue(highPriorityEvent)

    verify { controllerAdapter.enqueue(partition, highPriorityEvent, "{}", OutboxTaskPriority.HIGH) }
  }

  @Test
  fun `partitionInfos delegates to partition port`() {
    val info = OutboxPartitionInfo(
      key = "test-partition",
      status = OutboxPartitionStatus.ACTIVE,
      statusReason = null,
      pausedUntil = null,
    )
    every { partitionPort.findAllPartitions() } returns listOf(info)

    val result = clientAdapter.partitionInfos()

    assertThat(result).containsExactly(info)
    verify { partitionPort.findAllPartitions() }
  }

  @Test
  fun `partitionInfos returns empty list when no partitions exist`() {
    every { partitionPort.findAllPartitions() } returns emptyList()

    val result = clientAdapter.partitionInfos()

    assertThat(result).isEmpty()
  }
}
