package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.TaskPort
import de.chrgroth.quarkus.outbox.domain.port.out.OutboxRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class TaskAdapter(
  private val repository: OutboxRepository,
  private val coroutinesAdapter: CoroutinesAdapter,
  private val meterRegistry: MeterRegistry,
) : TaskPort {

  private val enqueuedCounters = ConcurrentHashMap<String, Counter>()

  override fun enqueue(
    partition: OutboxPartition,
    event: OutboxEvent,
    payload: String,
    priority: OutboxTaskPriority,
  ): Boolean {
    val inserted = repository.enqueue(partition, event, payload, priority)
    if (inserted) {
      coroutinesAdapter.wakeUp(partition)
      enqueuedCounters.getOrPut(partition.key) {
        meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key)
      }.increment()
    }
    return inserted
  }
}
