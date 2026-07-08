package de.chrgroth.quarkus.outbox.adapter.out.mongodb.startup

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.ArchivedTaskRepository
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.TaskRepository
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.event.Observes
import jakarta.inject.Inject
import mu.KLogging
import org.bson.Document

private data class OutboxIndex(
  val keys: Document,
  val name: String,
)

@ApplicationScoped
@Suppress("Unused", "UnusedParameter")
class IndexInitializationStarter {

  @Inject
  lateinit var tasks: TaskRepository

  @Inject
  lateinit var archive: ArchivedTaskRepository

  fun onStartup(@Observes event: StartupEvent) {
    logger.info { "Syncing outbox MongoDB indexes..." }

    syncIndexes(
      tasks.mongoCollection(),
      listOf(
        OutboxIndex(
          Document("partition", 1).append("status", 1).append("priorityOrder", 1).append("createdAt", 1).append("nextRetryAt", 1),
          "partition_1_status_1_priorityOrder_1_createdAt_1_nextRetryAt_1",
        ),
        OutboxIndex(
          Document("partition", 1).append("deduplicationKey", 1).append("status", 1),
          "partition_1_deduplicationKey_1_status_1",
        ),
        OutboxIndex(
          Document("status", 1),
          "status_1",
        ),
        OutboxIndex(
          Document("partition", 1),
          "partition_1",
        ),
        OutboxIndex(
          Document("partition", 1).append("eventType", 1),
          "partition_1_eventType_1",
        ),
        OutboxIndex(
          Document("partition", 1).append("priorityOrder", 1).append("createdAt", 1),
          "partition_1_priorityOrder_1_createdAt_1",
        ),
      ),
    )

    syncIndexes(
      archive.mongoCollection(),
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
      logger.info { "Creating missing index '${index.name}' for collection '$collectionName'" }
      collection.createIndex(index.keys, IndexOptions().name(index.name))
    }
  }

  companion object : KLogging()
}
