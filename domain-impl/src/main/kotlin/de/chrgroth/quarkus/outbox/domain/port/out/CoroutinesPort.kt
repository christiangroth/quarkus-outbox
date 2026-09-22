package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import kotlinx.coroutines.CoroutineScope

interface CoroutinesPort {

  fun getScope(): CoroutineScope

  /** Signals every worker of [partition] (see [ApplicationOutboxPartition.workerCount]). */
  fun signal(partition: ApplicationOutboxPartition)

  /** Suspends until worker [workerIndex] of [partition] is signalled. */
  suspend fun waitOnSignal(partition: ApplicationOutboxPartition, workerIndex: Int = 0)
}
