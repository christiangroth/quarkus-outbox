package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.ExecutionAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionAdapter
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant

@ApplicationScoped
@Suppress("Unused", "UnusedParameter", "SwallowedException")
class PartitionWorkerStarter(
  private val coroutinesPort: CoroutinesPort,
  private val repository: OutboxRepository,
  private val executionAdapter: ExecutionAdapter,
  private val partitionAdapter: PartitionAdapter,
  private val application: ApplicationPort,
) {

  fun onStart(@Observes @Priority(1) event: StartupEvent) {
    executionAdapter.resetStaleProcessingTasks()

    val partitions = application.getAllPartitions()
    partitions.forEach { partition ->
      startup(partition)
      startPartitionWorker(partition)
    }

    logger.info { "Outbox startup recovery complete for ${partitions.size} partition(s)" }
  }

  private fun startup(partition: OutboxPartition) {
    val partitionInfo = repository.findOrCreatePartition(partition)
    if (partitionInfo.status != OutboxPartitionStatus.PAUSED.name) {
      recoverActive(partition)
      return
    }

    val pausedUntil = partitionInfo.pausedUntil
    if (pausedUntil == null) {
      logger.info { "Partition paused endlessly: $partition" }
      return
    }

    val now = Instant.now()
    if (now.isAfter(pausedUntil)) {
      recoverActive(partition)
      logger.info { "Reactivated expired paused partition ${partition.key}" }
      return
    }

    logger.info { "Partition ${partition.key} still paused until $pausedUntil, scheduling delayed activation" }
    coroutinesPort.scope().launch {
      delay(pausedUntil.toEpochMilli() - now.toEpochMilli())
      logger.info { "Resuming partition ${partition.key} after delayed activation" }
      recoverActive(partition)
    }
  }

  private fun recoverActive(partition: OutboxPartition) {
    partitionAdapter.activatePartition(partition)
    coroutinesPort.wakeUp(partition)
  }

  private fun startPartitionWorker(partition: OutboxPartition) {
    logger.info { "Starting partition worker for ${partition.key}" }
    coroutinesPort.scope().launch {
      val throttleInterval = partition.throttleInterval
      while (isActive) {
        coroutinesPort.waitOnSignal(partition)

        var processed: Boolean
        do {
          processed = executionAdapter.dispatchTask(partition) { task ->
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


