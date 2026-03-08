package de.chrgroth.outbox

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

@ApplicationScoped
class OutboxQueryMetrics(
    private val meterRegistry: MeterRegistry,
    @param:ConfigProperty(name = "app.mongodb.slow-query-threshold-ms", defaultValue = "250")
    private val slowQueryThresholdMs: Long,
) {

    private val timers = ConcurrentHashMap<String, Timer>()
    private val slowQueryCounters = ConcurrentHashMap<String, Counter>()

    fun <T> timed(operation: String, block: () -> T): T {
        val startMs = System.currentTimeMillis()
        val result = block()
        val durationMs = System.currentTimeMillis() - startMs

        timers.getOrPut(operation) {
            Timer.builder("mongodb.query")
                .tag("operation", operation)
                .register(meterRegistry)
        }.record(durationMs, TimeUnit.MILLISECONDS)

        if (durationMs >= slowQueryThresholdMs) {
            logger.warn { "Slow MongoDB query detected: operation=$operation duration=${durationMs}ms threshold=${slowQueryThresholdMs}ms" }
            slowQueryCounters.getOrPut(operation) {
                meterRegistry.counter("mongodb.slow.queries", "operation", operation)
            }.increment()
        }

        return result
    }

    companion object : KLogging()
}
