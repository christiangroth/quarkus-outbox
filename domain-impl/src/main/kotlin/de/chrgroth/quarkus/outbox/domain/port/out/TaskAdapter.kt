package de.chrgroth.quarkus.outbox.domain.port.out

import de.chrgroth.quarkus.outbox.domain.OutboxEvent
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxTaskPriority
import de.chrgroth.quarkus.outbox.domain.port.`in`.TaskPort
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class TaskAdapter(
  private val repository: OutboxRepository,
  private val coroutinesPort: CoroutinesPort,
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
      coroutinesPort.signal(partition)
      enqueuedCounters.getOrPut(partition.key) {
        meterRegistry.counter("outbox_tasks_enqueued_total", "partition", partition.key)
      }.increment()
    }
    return inserted
  }
}
