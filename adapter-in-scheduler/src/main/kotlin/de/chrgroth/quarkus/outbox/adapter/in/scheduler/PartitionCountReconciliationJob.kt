package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.PartitionCountReconciliationPort
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.quarkus.scheduler.Scheduled
import io.quarkus.scheduler.ScheduledExecution
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
@Suppress("Unused")
class PartitionCountReconciliationJob(
  private val reconciliationPort: PartitionCountReconciliationPort,
  private val meterRegistry: MeterRegistry,
  @param:ConfigProperty(name = "outbox.reconciliation.enabled")
  private val enabled: Boolean,
) : Scheduled.SkipPredicate {

  private val reconciliationTimer = Timer.builder("outbox.reconciliation.duration")
    .description("Duration of the partition event type count reconciliation job")
    .register(meterRegistry)
  private val correctedCounter = meterRegistry.counter("outbox.reconciliation.corrected")

  override fun test(execution: ScheduledExecution): Boolean = !enabled

  @Scheduled(cron = "0 30 1 * * ?", skipExecutionIf = PartitionCountReconciliationJob::class)
  fun run() {
    logger.info { "Running outbox partition event type count reconciliation" }
    val sample = Timer.start(meterRegistry)
    val corrected = reconciliationPort.reconcileEventTypeCounts()
    sample.stop(reconciliationTimer)
    correctedCounter.increment(corrected.toDouble())
    logger.info { "Outbox reconciliation corrected drifted event type counts for $corrected partition(s)" }
  }

  companion object : KLogging()
}
