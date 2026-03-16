package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.ApplicationPort
import de.chrgroth.quarkus.outbox.domain.CoroutinesAdapter
import de.chrgroth.quarkus.outbox.domain.ExecutionAdapter
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.quarkus.runtime.StartupEvent
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant

@ApplicationScoped
@Suppress("Unused", "UnusedParameter")
class PartitionWorkerStarter(
  private val coroutinesAdapter: CoroutinesAdapter,
  private val repository: OutboxRepository,
  private val executionAdapter: ExecutionAdapter,
  private val application: ApplicationPort,
) {

  fun onStart(@Observes @Priority(1) event: StartupEvent) {

    // set all executing tasks back to pending
    executionAdapter.resetStaleProcessingTasks()

    application.getAllPartitions().forEach { partition ->
      startup(partition)
    }

    logger.info { "Outbox startup recovery complete for ${application.getAllPartitions().size} partition(s)" }
  }

  private fun startup(partition: OutboxPartition) {

    // TODO findOrCreate so we always have a partition document
    val partitionInfo = repository.findPartition(partition)
    // TODO status should be enum
    // mo document means active
    if (partitionInfo == null || partitionInfo.status != OutboxPartitionStatus.PAUSED.name) {
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
    coroutinesAdapter.scope().launch {
      delay(pausedUntil.toEpochMilli() - now.toEpochMilli())
      logger.info { "Resuming partition ${partition.key} after delayed activation" }
      recoverActive(partition)
    }
  }

  private fun recoverActive(partition: OutboxPartition) {
    executionAdapter.activatePartition(partition)
    coroutinesAdapter.wakeUp(partition)
  }

  companion object : KLogging()
}
