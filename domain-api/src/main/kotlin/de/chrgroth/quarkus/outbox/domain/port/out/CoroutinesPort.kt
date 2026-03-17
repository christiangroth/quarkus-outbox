package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import kotlinx.coroutines.CoroutineScope

interface CoroutinesPort {

  fun scope(): CoroutineScope

  fun wakeUp(partition: OutboxPartition)

  suspend fun waitOnSignal(partition: OutboxPartition)
}
