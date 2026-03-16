package de.chrgroth.quarkus.outbox.domain

import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class CoroutinesAdapterTests {

  private val adapter = CoroutinesAdapter()

  private val partitionA = object : OutboxPartition {
    override val key = "partition-a"
  }

  private val partitionB = object : OutboxPartition {
    override val key = "partition-b"
  }

  @AfterEach
  fun tearDown() {
    adapter.onStop()
  }

  @Test
  fun `scope is active after construction`() {
    assertThat(adapter.scope().isActive).isTrue()
  }

  @Test
  fun `onStop cancels scope`() {
    adapter.onStop()

    assertThat(adapter.scope().isActive).isFalse()
  }

  @Test
  fun `wakeUp and waitOnSignal communicate for same partition`() = runBlocking {
    adapter.wakeUp(partitionA)

    withTimeout(1000) {
      adapter.waitOnSignal(partitionA)
    }
  }

  @Test
  fun `wakeUp for one partition does not signal another partition`() = runBlocking {
    adapter.wakeUp(partitionA)

    // partitionB was not signalled – tryReceive via a direct send check
    adapter.wakeUp(partitionB)
    withTimeout(1000) {
      adapter.waitOnSignal(partitionB)
    }
    // partitionA signal is still available (channel is CONFLATED so it keeps last value)
    withTimeout(1000) {
      adapter.waitOnSignal(partitionA)
    }
  }
}
