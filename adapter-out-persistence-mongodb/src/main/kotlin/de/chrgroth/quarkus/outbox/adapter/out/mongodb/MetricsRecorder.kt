package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Records MongoDB operation timings using the same metric names as the consuming applications
 * (`mongodb.query` timer and `mongodb.slow.queries` counter, both tagged with `operation`), so the
 * outbox's MongoDB work shows up in the applications' existing dashboards. All operation names used by
 * this library are prefixed with `outbox.` to distinguish them from application operations.
 */
@ApplicationScoped
class MetricsRecorder(
  private val meterRegistry: MeterRegistry,
  @param:ConfigProperty(name = "outbox.mongodb.slow-query-threshold-ms")
  private val slowQueryThresholdMs: Long,
) {

  private val timers = ConcurrentHashMap<String, Timer>()
  private val slowQueryCounters = ConcurrentHashMap<String, Counter>()

  fun <T> timed(operation: String, block: () -> T): T {
    val startNanos = System.nanoTime()
    try {
      return block()
    } finally {
      record(operation, System.nanoTime() - startNanos)
    }
  }

  private fun record(operation: String, durationNanos: Long) {
    timers.getOrPut(operation) {
      Timer.builder(QUERY_TIMER_NAME)
        .tag(OPERATION_TAG, operation)
        .register(meterRegistry)
    }.record(durationNanos, TimeUnit.NANOSECONDS)

    val durationMs = TimeUnit.NANOSECONDS.toMillis(durationNanos)
    if (durationMs >= slowQueryThresholdMs) {
      logger.warn { "Slow MongoDB query detected: operation=$operation duration=${durationMs}/${slowQueryThresholdMs}ms" }
      slowQueryCounters.getOrPut(operation) {
        meterRegistry.counter(SLOW_QUERY_COUNTER_NAME, OPERATION_TAG, operation)
      }.increment()
    }
  }

  companion object : KLogging() {
    const val QUERY_TIMER_NAME = "mongodb.query"
    const val SLOW_QUERY_COUNTER_NAME = "mongodb.slow.queries"
    const val OPERATION_TAG = "operation"
  }
}
