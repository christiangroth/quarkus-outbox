package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import java.time.Instant

@ApplicationScoped
class ArchiverAdapter(
  private val archivePort: ArchivedTaskRepositoryPort,
  private val meterRegistry: MeterRegistry,
) : ArchiverPort {

  init {
    Gauge.builder("outbox_archive_count", archivePort) { it.count().toDouble() }
      .description("Current number of tasks in the archive")
      .register(meterRegistry)
  }

  override fun deleteOlderThan(cutoff: Instant): Long =
    archivePort.deleteOlderThan(cutoff)

  companion object : KLogging()
}
