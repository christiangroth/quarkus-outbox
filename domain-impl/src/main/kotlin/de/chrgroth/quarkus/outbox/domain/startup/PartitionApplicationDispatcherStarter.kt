package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.CoroutinesAdapter
import de.chrgroth.quarkus.outbox.domain.ExecutionAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KLogging

@ApplicationScoped
@Suppress("Unused", "UnusedParameter", "SwallowedException")
class PartitionApplicationDispatcherStarter(
  private val coroutinesAdapter: CoroutinesAdapter,
  private val executionAdapter: ExecutionAdapter,
  private val application: ApplicationPort,
) {

  fun onStart(@Observes event: StartupEvent) {
    application.getAllPartitions().forEach { partition ->
      startPartitionWorker(partition)
    }
  }

  private fun startPartitionWorker(partition: OutboxPartition) {
    logger.info { "Starting partition worker for ${partition.key}" }
    coroutinesAdapter.scope().launch {
      val throttleInterval = partition.throttleInterval
      while (isActive) {
        coroutinesAdapter.waitOnSignal(partition)

        var processed: Boolean
        do {
          processed = executionAdapter.dispatchNext(partition) { task ->
            application.dispatch(task)
          }

          if (processed && throttleInterval != null) {
            delay(throttleInterval.toMillis())
          }
        } while (processed && isActive)
      }
    }
  }

  companion object : KLogging()
}
