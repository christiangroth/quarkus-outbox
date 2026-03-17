package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TaskAdapterTests {

  private val repository: OutboxRepository = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val coroutinesPort: CoroutinesPort = mockk {
    every { wakeUp(any()) } just runs
  }

  private val adapter = TaskAdapter(repository, coroutinesPort, meterRegistry)

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  private fun testEvent() = object : OutboxEvent {
    override val key = "TEST_EVENT"
    override fun deduplicationKey() = "dedup-key"
  }

  @Test
  fun `enqueue signals partition and increments counter when task is inserted`() {
    every { repository.enqueue(partition, any(), any(), any()) } returns true

    val result = adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isTrue()
    verify { coroutinesPort.wakeUp(partition) }
    assertThat(meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key).count()).isEqualTo(1.0)
  }

  @Test
  fun `enqueue does not signal or increment counter when task is rejected due to deduplication`() {
    every { repository.enqueue(partition, any(), any(), any()) } returns false

    val result = adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.NORMAL)

    assertThat(result).isFalse()
    verify(exactly = 0) { coroutinesPort.wakeUp(any()) }
    assertThat(meterRegistry.find("outbox_tasks_enqueued_total").counter()).isNull()
  }

  @Test
  fun `enqueue passes priority to repository`() {
    every { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) } returns true

    adapter.enqueue(partition, testEvent(), "payload", OutboxTaskPriority.HIGH)

    verify { repository.enqueue(partition, any(), any(), OutboxTaskPriority.HIGH) }
  }
}
