package de.chrgroth.quarkus.outbox.adapter.out.executor

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test

class CoroutinesAdapterTests {

  private val adapter = CoroutinesAdapter()

  private val partitionA = object : ApplicationOutboxPartition {
    override val key = "partition-a"
  }

  private val partitionB = object : ApplicationOutboxPartition {
    override val key = "partition-b"
  }

  @AfterEach
  fun tearDown() {
    adapter.onStop()
  }

  @Test
  fun `scope is active after construction`() {
    assertThat(adapter.getScope().isActive).isTrue()
  }

  @Test
  fun `onStop cancels scope`() {
    adapter.onStop()

    assertThat(adapter.getScope().isActive).isFalse()
  }

  @Test
  fun `wakeUp and waitOnSignal communicate for same partition`() {
    runBlocking {
      adapter.signal(partitionA)

      var signalReceived = false
      withTimeout(1000) {
        adapter.waitOnSignal(partitionA)
        signalReceived = true
      }

      assertThat(signalReceived).isTrue()
    }
  }

  @Test
  fun `exception in one child coroutine does not cancel the scope`() {
    runBlocking {
      adapter.getScope().launch {
        throw IllegalStateException("boom")
      }.join()

      assertThat(adapter.getScope().isActive).isTrue()
    }
  }

  @Test
  fun `wakeUp for one partition does not signal another partition`() {
    runBlocking {
      adapter.signal(partitionA)

      var partitionBSignalled = false
      val job = launch {
        try {
          withTimeout(100) {
            adapter.waitOnSignal(partitionB)
            partitionBSignalled = true
          }
        } catch (_: kotlinx.coroutines.TimeoutCancellationException) {
          // expected – partitionB was not signalled
        }
      }
      job.join()

      assertThat(partitionBSignalled).isFalse()
    }
  }
}
