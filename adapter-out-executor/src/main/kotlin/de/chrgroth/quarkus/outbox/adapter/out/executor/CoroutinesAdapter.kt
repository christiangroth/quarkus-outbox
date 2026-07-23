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
  private val channels: MutableMap<String, Channel<Unit>> = ConcurrentHashMap()

  override fun getScope() = scope

  override fun signal(partition: ApplicationOutboxPartition) {
    channelFor(partition).trySend(Unit)
  }

  override suspend fun waitOnSignal(partition: ApplicationOutboxPartition) {
    channelFor(partition).receive()
  }

  private fun channelFor(partition: ApplicationOutboxPartition): Channel<Unit> =
    channels.getOrPut(partition.key) {
      Channel(Channel.CONFLATED)
    }

  @PreDestroy
  fun onStop() {
    scope.cancel()
  }
}
