package de.chrgroth.quarkus.outbox.domain

import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

// TODO out port?
@ApplicationScoped
@Suppress("Unused")
class CoroutinesAdapter {

  private val scope = CoroutineScope(Dispatchers.IO)
  private val channels: MutableMap<String, Channel<Unit>> = ConcurrentHashMap()

  fun scope() = scope

  fun wakeUp(partition: OutboxPartition) {
    channelFor(partition).trySend(Unit)
  }

  suspend fun waitOnSignal(partition: OutboxPartition) {
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
