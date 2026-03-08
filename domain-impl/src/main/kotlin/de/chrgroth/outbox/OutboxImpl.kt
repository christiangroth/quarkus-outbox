package de.chrgroth.outbox

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Any
import jakarta.enterprise.inject.Instance
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

@ApplicationScoped
class OutboxImpl(
    private val repository: OutboxRepository,
    private val wakeupService: OutboxWakeupService,
    private val meterRegistry: MeterRegistry,
    @param:Any private val partitionObservers: Instance<OutboxPartitionObserver>,
) : Outbox {

    private val scope = CoroutineScope(Dispatchers.IO)

    private val processor = OutboxProcessor(repository) { partition, retryAfter ->
        if (partition.pauseOnRateLimit) {
            partitionObservers.forEach { it.onPartitionPaused(partition) }
        }
        scope.launch {
            delay(retryAfter.toMillis())
            if (partition.pauseOnRateLimit) {
                activatePartition(partition)
            }
            wakeupService.signal(partition)
        }
    }
    
    private val enqueuedCounters = ConcurrentHashMap<String, Counter>()
    private val processedCounters = ConcurrentHashMap<String, Counter>()
    private val failedCounters = ConcurrentHashMap<String, Counter>()
    private val rateLimitedCounters = ConcurrentHashMap<String, Counter>()
    private val partitionStatusGauges = ConcurrentHashMap<String, AtomicInteger>()

    override fun enqueue(
        partition: OutboxPartition,
        event: OutboxEvent,
        payload: String,
        priority: OutboxTaskPriority,
    ): Boolean {
        val inserted = repository.enqueue(partition, event, payload, priority)
        if (inserted) {
            wakeupService.signal(partition)
            enqueuedCounters.getOrPut(partition.key) {
                meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key)
            }.increment()
        }
        return inserted
    }

    override fun processNext(partition: OutboxPartition, dispatch: (OutboxTask) -> OutboxTaskResult): Boolean =
        processor.processNext(partition) { task ->
            val result = dispatch(task)
            when (result) {
                is OutboxTaskResult.Success -> processedCounters.getOrPut(partition.key) {
                    meterRegistry.counter("outbox_tasks_processed_total", "partition", partition.key)
                }.increment()
                is OutboxTaskResult.RateLimited -> {
                    rateLimitedCounters.getOrPut(partition.key) {
                        meterRegistry.counter("outbox_tasks_rate_limited_total", "partition", partition.key)
                    }.increment()
                    if (partition.pauseOnRateLimit) {
                        getOrCreatePartitionStatusGauge(partition).set(0)
                    }
                }
                is OutboxTaskResult.Failed -> failedCounters.getOrPut(partition.key) {
                    meterRegistry.counter("outbox_tasks_failed_total", "partition", partition.key)
                }.increment()
            }
            result
        }

    override fun signal(partition: OutboxPartition) = wakeupService.signal(partition)

    override fun getOrCreateChannel(partition: OutboxPartition) = wakeupService.getOrCreate(partition)

    override fun resetStaleProcessingTasks() = repository.resetStaleProcessingTasks()

    override fun findPartition(partition: OutboxPartition) = repository.findPartition(partition)

    override fun archiveFailedTasks() = repository.archiveFailedTasks()

    override fun activatePartition(partition: OutboxPartition) {
        repository.activatePartition(partition)
        getOrCreatePartitionStatusGauge(partition).set(1)
        partitionObservers.forEach { it.onPartitionActivated(partition) }
    }

    private fun getOrCreatePartitionStatusGauge(partition: OutboxPartition): AtomicInteger =
        partitionStatusGauges.getOrPut(partition.key) {
            val initialStatus = repository.findPartition(partition)
                ?.let { if (it.status == OutboxPartitionStatus.ACTIVE.name) 1 else 0 } ?: 1
            AtomicInteger(initialStatus).also { gauge ->
                Gauge.builder("outbox_partition_status", gauge) { it.get().toDouble() }
                    .tag("partition", partition.key)
                    .description("Outbox partition status: 1=active, 0=paused")
                    .register(meterRegistry)
            }
        }

    @PreDestroy
    fun onStop() {
        scope.cancel()
    }
}
