package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@ApplicationScoped
class MetricsRecorder(
  private val meterRegistry: MeterRegistry,
  @param:ConfigProperty(name = "app.outbox.mongodb.slow-query-threshold-ms")
  private val slowQueryThresholdMs: Long,
) {

  private val timers = ConcurrentHashMap<String, Timer>()
  private val slowQueryCounters = ConcurrentHashMap<String, Counter>()

  fun <T> timed(operation: String, block: () -> T): T {
    val startMs = System.currentTimeMillis()
    val result = block()
    val durationMs = System.currentTimeMillis() - startMs

    timers.getOrPut(operation) {
      Timer.builder("outbox.mongodb.query.duration")
        .tag("operation", operation)
        .register(meterRegistry)
    }.record(durationMs, TimeUnit.MILLISECONDS)

    if (durationMs >= slowQueryThresholdMs) {
      logger.warn { "Slow MongoDB query detected: operation=$operation duration=${durationMs}/${slowQueryThresholdMs}ms" }
      slowQueryCounters.getOrPut(operation) {
        meterRegistry.counter("outbox.mongodb.query.slow.count", "operation", operation)
      }.increment()
    }

    return result
  }

  companion object : KLogging()
}
