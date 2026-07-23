package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty

@ApplicationScoped
@Suppress("Unused", "UnusedParameter")
class ArchiveCleanupStarter(
  private val archivePort: ArchivedTaskRepositoryPort,
  @param:ConfigProperty(name = "outbox.archive.enabled", defaultValue = "true")
  private val archiveEnabled: Boolean,
) {

  fun onStart(@Observes event: StartupEvent) {
    if (archiveEnabled) {
      return
    }

    val count = archivePort.count()
    if (count == 0L) {
      return
    }

    logger.info { "Archive is disabled but still holds $count entries, clearing archive collection" }
    val deleted = archivePort.deleteAll()
    logger.info { "Cleared $deleted entries from the archive collection" }
  }

  companion object : KLogging()
}
