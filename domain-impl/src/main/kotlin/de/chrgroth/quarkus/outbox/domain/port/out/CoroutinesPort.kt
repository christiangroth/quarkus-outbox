package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import kotlinx.coroutines.CoroutineScope

interface CoroutinesPort {

  fun getScope(): CoroutineScope

  fun signal(partition: ApplicationOutboxPartition)

  suspend fun waitOnSignal(partition: ApplicationOutboxPartition)
}
