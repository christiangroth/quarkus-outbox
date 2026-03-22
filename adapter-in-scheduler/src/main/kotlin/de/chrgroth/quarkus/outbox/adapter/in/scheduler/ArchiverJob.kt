package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.time.Instant
import java.time.temporal.ChronoUnit

@ApplicationScoped
@Suppress("Unused")
class ArchiverJob(
  private val archiverPort: ArchiverPort,
  private val meterRegistry: MeterRegistry,
  @param:ConfigProperty(name = "outbox.archive.enabled", defaultValue = "true")
  private val enabled: Boolean,
  @param:ConfigProperty(name = "outbox.archive.retention-days")
  private val retentionDays: Long,
) {

  private val cleanupTimer = Timer.builder("outbox_archive_cronjob_duration")
    .description("Duration of archive cleanup cron job")
    .register(meterRegistry)
  private val deletionCounter = meterRegistry.counter("outbox_archive_tasks_deleted_total")

  @Scheduled(cron = "0 0 1 * * ?")
  fun run() {
    if (!enabled) {
      logger.info { "Archive cleanup is disabled, skipping run" }
      return
    }

    logger.info { "Running outbox archive cleanup (retention: $retentionDays days)" }
    val cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS)
    val sample = Timer.start(meterRegistry)
    val deletionCount = archiverPort.deleteOlderThan(cutoff)
    sample.stop(cleanupTimer)
    deletionCounter.increment(deletionCount.toDouble())
    logger.info { "Outbox archive cleanup deleted $deletionCount entries older than $cutoff" }
  }

  companion object : KLogging()
}
