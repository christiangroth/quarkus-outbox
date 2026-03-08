package de.chrgroth.outbox

import io.quarkus.runtime.StartupEvent
import jakarta.annotation.PreDestroy
import jakarta.annotation.Priority
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KLogging
import java.time.Instant

@ApplicationScoped
@Suppress("Unused", "UnusedParameter")
class OutboxStartupRecovery(
    private val outbox: Outbox,
    private val dispatcher: OutboxTaskDispatcher,
) {

    private val scope = CoroutineScope(Dispatchers.IO)

    fun onStart(@Observes @Priority(1) event: StartupEvent) {
        outbox.resetStaleProcessingTasks()
        val now = Instant.now()
        dispatcher.partitions.forEach { partition ->
            val partitionInfo = outbox.findPartition(partition)
            // partitionInfo.status is persisted as a String; compare via .name
            if (partitionInfo?.status == OutboxPartitionStatus.PAUSED.name) {
                val pausedUntil = partitionInfo.pausedUntil
                if (pausedUntil != null && pausedUntil.isAfter(now)) {
                    val delayMs = pausedUntil.toEpochMilli() - now.toEpochMilli()
                    scope.launch {
                        delay(delayMs)
                        outbox.activatePartition(partition)
                        outbox.signal(partition)
                        logger.info { "Resumed partition ${partition.key} after delayed activation" }
                    }
                    logger.info { "Partition ${partition.key} still paused until $pausedUntil, scheduled delayed activation" }
                } else {
                    outbox.activatePartition(partition)
                    outbox.signal(partition)
                    logger.info { "Reactivated expired paused partition ${partition.key}" }
                }
            } else {
                outbox.activatePartition(partition)
                outbox.signal(partition)
            }
        }
        logger.info { "Outbox startup recovery complete for ${dispatcher.partitions.size} partition(s)" }
    }

    @PreDestroy
    fun onStop() {
        scope.cancel()
    }

    companion object : KLogging()
}
