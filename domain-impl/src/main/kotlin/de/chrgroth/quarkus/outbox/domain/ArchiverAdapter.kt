package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import java.time.Instant

@ApplicationScoped
class ArchiverAdapter(
  private val archivePort: ArchivedTaskRepositoryPort,
) : ArchiverPort {

  override fun deleteOlderThan(cutoff: Instant): Long =
    archivePort.deleteOlderThan(cutoff)

  companion object : KLogging()
}
