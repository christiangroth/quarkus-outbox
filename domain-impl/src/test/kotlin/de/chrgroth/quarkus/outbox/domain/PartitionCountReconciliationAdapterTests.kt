package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PartitionCountReconciliationAdapterTests {

  private val partitionPort: PartitionRepositoryPort = mockk()
  private val taskPort: TaskRepositoryPort = mockk()
  private val adapter = PartitionCountReconciliationAdapter(partitionPort, taskPort)

  private fun partitionInfo(key: String, eventPerTypeCount: Map<String, Long>?) = OutboxPartitionInfo(
    key = key,
    status = OutboxPartitionStatus.ACTIVE,
    statusReason = null,
    pausedUntil = null,
    eventCount = eventPerTypeCount?.values?.sum() ?: 0L,
    eventPerTypeCount = eventPerTypeCount,
  )

  @Test
  fun `reconcileEventTypeCounts leaves partition untouched when persisted counts match actual counts`() {
    every { partitionPort.findAllPartitions() } returns listOf(partitionInfo("p1", mapOf("TYPE_A" to 3L)))
    every { taskPort.countByEventType("p1") } returns mapOf("TYPE_A" to 3L)

    val corrected = adapter.reconcileEventTypeCounts()

    assertThat(corrected).isEqualTo(0L)
    verify(exactly = 0) { partitionPort.replaceEventTypeCounts(any(), any()) }
  }

  @Test
  fun `reconcileEventTypeCounts overwrites drifted counts with actual counts`() {
    every { partitionPort.findAllPartitions() } returns listOf(partitionInfo("p1", mapOf("TYPE_A" to 5L)))
    every { taskPort.countByEventType("p1") } returns mapOf("TYPE_A" to 3L)
    every { partitionPort.replaceEventTypeCounts(any(), any()) } just runs

    val corrected = adapter.reconcileEventTypeCounts()

    assertThat(corrected).isEqualTo(1L)
    verify { partitionPort.replaceEventTypeCounts(match { it.key == "p1" }, mapOf("TYPE_A" to 3L)) }
  }

  @Test
  fun `reconcileEventTypeCounts treats missing persisted counts as empty`() {
    every { partitionPort.findAllPartitions() } returns listOf(partitionInfo("p1", null))
    every { taskPort.countByEventType("p1") } returns mapOf("TYPE_A" to 1L)
    every { partitionPort.replaceEventTypeCounts(any(), any()) } just runs

    val corrected = adapter.reconcileEventTypeCounts()

    assertThat(corrected).isEqualTo(1L)
    verify { partitionPort.replaceEventTypeCounts(match { it.key == "p1" }, mapOf("TYPE_A" to 1L)) }
  }

  @Test
  fun `reconcileEventTypeCounts corrects only drifted partitions across multiple partitions`() {
    every { partitionPort.findAllPartitions() } returns listOf(
      partitionInfo("p1", mapOf("TYPE_A" to 3L)),
      partitionInfo("p2", mapOf("TYPE_B" to 9L)),
    )
    every { taskPort.countByEventType("p1") } returns mapOf("TYPE_A" to 3L)
    every { taskPort.countByEventType("p2") } returns mapOf("TYPE_B" to 4L)
    every { partitionPort.replaceEventTypeCounts(any(), any()) } just runs

    val corrected = adapter.reconcileEventTypeCounts()

    assertThat(corrected).isEqualTo(1L)
    verify(exactly = 0) { partitionPort.replaceEventTypeCounts(match { it.key == "p1" }, any()) }
    verify { partitionPort.replaceEventTypeCounts(match { it.key == "p2" }, mapOf("TYPE_B" to 4L)) }
  }

  @Test
  fun `reconcileEventTypeCounts returns zero when there are no partitions`() {
    every { partitionPort.findAllPartitions() } returns emptyList()

    val corrected = adapter.reconcileEventTypeCounts()

    assertThat(corrected).isEqualTo(0L)
    verify(exactly = 0) { partitionPort.replaceEventTypeCounts(any(), any()) }
  }
}
