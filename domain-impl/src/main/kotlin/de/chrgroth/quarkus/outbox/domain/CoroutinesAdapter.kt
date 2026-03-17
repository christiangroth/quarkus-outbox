package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
@Suppress("Unused")
class CoroutinesAdapter : CoroutinesPort {

  private val scope = CoroutineScope(Dispatchers.IO)
  private val channels: MutableMap<String, Channel<Unit>> = ConcurrentHashMap()

  override fun scope() = scope

  override fun wakeUp(partition: OutboxPartition) {
    channelFor(partition).trySend(Unit)
  }

  override suspend fun waitOnSignal(partition: OutboxPartition) {
    channelFor(partition).receive()
  }

  private fun channelFor(partition: OutboxPartition): Channel<Unit> =
    channels.getOrPut(partition.key) {
      Channel(Channel.CONFLATED)
    }

  @PreDestroy
  fun onStop() {
    scope.cancel()
  }
}

