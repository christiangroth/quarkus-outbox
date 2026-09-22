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

  @Test
  fun `signal broadcasts to every worker of a partition with multiple workers`() {
    val multiWorkerPartition = object : ApplicationOutboxPartition {
      override val key = "multi-worker-partition"
      override val workerCount = 3
    }

    runBlocking {
      adapter.signal(multiWorkerPartition)

      withTimeout(1000) {
        adapter.waitOnSignal(multiWorkerPartition, 0)
        adapter.waitOnSignal(multiWorkerPartition, 1)
        adapter.waitOnSignal(multiWorkerPartition, 2)
      }
    }
  }

  @Test
  fun `waitOnSignal for a workerIndex of one partition does not receive a signal for the same workerIndex of another partition`() {
    val partitionAMultiWorker = object : ApplicationOutboxPartition {
      override val key = "isolated-partition-a"
      override val workerCount = 2
    }
    val partitionBMultiWorker = object : ApplicationOutboxPartition {
      override val key = "isolated-partition-b"
      override val workerCount = 2
    }

    runBlocking {
      adapter.signal(partitionAMultiWorker)

      var partitionBWorker1Signalled = false
      val job = launch {
        try {
          withTimeout(100) {
            adapter.waitOnSignal(partitionBMultiWorker, 1)
            partitionBWorker1Signalled = true
          }
        } catch (_: kotlinx.coroutines.TimeoutCancellationException) {
          // expected - partition B worker 1 was not signalled
        }
      }
      job.join()

      assertThat(partitionBWorker1Signalled).isFalse()
    }
  }
}
