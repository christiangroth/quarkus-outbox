package de.chrgroth.outbox

import com.mongodb.client.model.IndexOptions
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.inject.Inject
import mu.KLogging
import org.bson.Document

@ApplicationScoped
@Suppress("UnusedParameter")
class OutboxIndexInitializer {

    @Inject
    lateinit var outboxDocumentRepository: OutboxDocumentRepository

    @Inject
    lateinit var outboxArchiveDocumentRepository: OutboxArchiveDocumentRepository

    fun onStartup(@Observes event: StartupEvent) {
        logger.info { "Ensuring outbox MongoDB indexes..." }

        // claim() and enqueue() filter by partition+status; claim() also filters nextRetryAt
        outboxDocumentRepository.mongoCollection().createIndex(
            Document("partition", 1).append("status", 1).append("nextRetryAt", 1),
            IndexOptions().name("partition_1_status_1_nextRetryAt_1"),
        )

        // enqueue() dedup check filters by partition+deduplicationKey+status
        outboxDocumentRepository.mongoCollection().createIndex(
            Document("partition", 1).append("deduplicationKey", 1).append("status", 1),
            IndexOptions().name("partition_1_deduplicationKey_1_status_1"),
        )

        // deleteArchiveEntriesOlderThan() filters by completedAt
        outboxArchiveDocumentRepository.mongoCollection().createIndex(
            Document("completedAt", 1),
            IndexOptions().name("completedAt_1"),
        )

        logger.info { "Outbox MongoDB indexes ready." }
    }

    companion object : KLogging()
}
