package de.chrgroth.quarkus.outbox.adapter.out.executor

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
@Suppress("Unused")
class CoroutinesAdapter : CoroutinesPort {

  private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
  private val channels: MutableMap<String, MutableMap<Int, Channel<Unit>>> = ConcurrentHashMap()

  override fun getScope() = scope

  override fun signal(partition: ApplicationOutboxPartition) {
    val workerCount = maxOf(1, partition.workerCount)
    for (workerIndex in 0 until workerCount) {
      channelFor(partition, workerIndex).trySend(Unit)
    }
  }

  override suspend fun waitOnSignal(partition: ApplicationOutboxPartition, workerIndex: Int) {
    channelFor(partition, workerIndex).receive()
  }

  private fun channelFor(partition: ApplicationOutboxPartition, workerIndex: Int): Channel<Unit> =
    channels.getOrPut(partition.key) { ConcurrentHashMap() }.getOrPut(workerIndex) {
      Channel(Channel.CONFLATED)
    }

  @PreDestroy
  fun onStop() {
    scope.cancel()
  }
}
