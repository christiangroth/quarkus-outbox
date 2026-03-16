package de.chrgroth.quarkus.outbox.domain

import com.mongodb.client.MongoCollection
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
        logger.info { "Syncing outbox MongoDB indexes..." }

        // claim() and enqueue() filter by partition+status; claim() also filters nextRetryAt
        // enqueue() dedup check filters by partition+deduplicationKey+status
        syncIndexes(
            outboxDocumentRepository.mongoCollection(),
            listOf(
                OutboxIndex(
                    Document("partition", 1).append("status", 1).append("nextRetryAt", 1),
                    "partition_1_status_1_nextRetryAt_1",
                ),
                OutboxIndex(
                    Document("partition", 1).append("deduplicationKey", 1).append("status", 1),
                    "partition_1_deduplicationKey_1_status_1",
                ),
            ),
        )

        // deleteArchiveEntriesOlderThan() filters by completedAt
        syncIndexes(
            outboxArchiveDocumentRepository.mongoCollection(),
            listOf(
                OutboxIndex(
                    Document("completedAt", 1),
                    "completedAt_1",
                ),
            ),
        )

        logger.info { "Outbox MongoDB indexes synced." }
    }

    private fun syncIndexes(collection: MongoCollection<*>, desiredIndexes: List<OutboxIndex>) {
        val collectionName = collection.namespace.collectionName
        val existingNames = collection.listIndexes()
            .mapNotNull { it["name"]?.toString() }
            .filter { it != "_id_" }
            .toSet()

        val desiredNames = desiredIndexes.map { it.name }.toSet()

        (existingNames - desiredNames).forEach { name ->
            logger.info { "Dropping unexpected index '$name' from collection '$collectionName'" }
            collection.dropIndex(name)
        }

        desiredIndexes.filter { it.name !in existingNames }.forEach { index ->
            collection.createIndex(index.keys, IndexOptions().name(index.name))
        }
    }

    private data class OutboxIndex(val keys: Document, val name: String)

    companion object : KLogging()
}
