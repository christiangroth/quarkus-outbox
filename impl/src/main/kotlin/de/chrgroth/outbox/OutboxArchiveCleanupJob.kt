package de.chrgroth.outbox

import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped
import mu.KLogging
import org.eclipse.microprofile.config.inject.ConfigProperty
import java.time.Instant
import java.time.temporal.ChronoUnit

@ApplicationScoped
@Suppress("Unused")
class OutboxArchiveCleanupJob(
    private val repository: MongoOutboxRepository,
    @param:ConfigProperty(name = "app.outbox.archive-retention-days")
    private val retentionDays: Long,
) {

    @Scheduled(cron = "0 0 1 * * ?")
    fun run() {
        logger.info { "Running outbox archive cleanup (retention: $retentionDays days)" }
        val cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS)
        val deleted = repository.deleteArchiveEntriesOlderThan(cutoff)
        logger.info { "Outbox archive cleanup deleted $deleted entries older than $cutoff" }
    }

    companion object : KLogging()
}
