package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxDispatcher
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxControllerAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.CoroutinesPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant

@ApplicationScoped
@Suppress("Unused", "UnusedParameter", "SwallowedException")
class PartitionWorkerStarter(
  private val coroutinesPort: CoroutinesPort,
  private val partitionPort: PartitionRepositoryPort,
  private val executionAdapter: OutboxControllerAdapter,
  private val application: ApplicationOutboxDispatcher,
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

  private fun startup(partition: ApplicationOutboxPartition) {
    val partitionInfo = partitionPort.findOrCreate(partition)
    if (partitionInfo.status != OutboxPartitionStatus.PAUSED) {
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
    coroutinesPort.getScope().launch {
      delay(pausedUntil.toEpochMilli() - now.toEpochMilli())
      logger.info { "Resuming partition ${partition.key} after delayed activation" }
      recoverActive(partition)
    }
  }

  private fun recoverActive(partition: ApplicationOutboxPartition) {
    executionAdapter.activatePartition(partition)
    executionAdapter.scheduleRetryWakeupIfNeeded(partition)
    coroutinesPort.signal(partition)
  }

  private fun startPartitionWorker(partition: ApplicationOutboxPartition) {
    val workerCount = maxOf(1, partition.workerCount)
    logger.info { "Starting $workerCount partition worker(s) for ${partition.key}" }
    repeat(workerCount) { workerIndex ->
      startPartitionWorker(partition, workerIndex)
    }
  }

  @Suppress("TooGenericExceptionCaught")
  private fun startPartitionWorker(partition: ApplicationOutboxPartition, workerIndex: Int) {
    coroutinesPort.getScope().launch {
      val throttleInterval = partition.throttleInterval
      while (isActive) {
        try {
          coroutinesPort.waitOnSignal(partition, workerIndex)

          var processed: Boolean
          do {
            processed = executionAdapter.dispatchTask(partition, workerIndex)

            if (processed && throttleInterval != null) {
              delay(throttleInterval.toMillis())
            }
          } while (processed && isActive)
        } catch (e: CancellationException) {
          throw e
        } catch (e: Exception) {
          logger.error(e) { "Unexpected error in partition worker $workerIndex for ${partition.key}, continuing" }
        }
      }
    }
  }

  companion object : KLogging()
}

