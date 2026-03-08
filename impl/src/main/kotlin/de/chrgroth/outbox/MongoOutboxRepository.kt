package de.chrgroth.outbox

import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.Filters
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.Updates
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import mu.KLogging
import java.time.Instant
import java.util.UUID

@ApplicationScoped
class MongoOutboxRepository : OutboxRepository {

    @Inject
    lateinit var outboxDocumentRepository: OutboxDocumentRepository

    @Inject
    lateinit var outboxPartitionDocumentRepository: OutboxPartitionDocumentRepository

    @Inject
    lateinit var outboxArchiveDocumentRepository: OutboxArchiveDocumentRepository

    @Inject
    lateinit var outboxQueryMetrics: OutboxQueryMetrics

    override fun claim(partition: OutboxPartition): OutboxTask? {
        val partitionDoc = findPartition(partition)
        if (partitionDoc != null && partitionDoc.status == OutboxPartitionStatus.PAUSED.name) {
            return null
        }

        val now = Instant.now()
        val doc = outboxQueryMetrics.timed("outbox.claim") {
            outboxDocumentRepository.mongoCollection().findOneAndUpdate(
                Filters.and(
                    Filters.eq("partition", partition.key),
                    Filters.eq("status", OutboxTaskStatus.PENDING.name),
                    Filters.or(
                        Filters.exists("nextRetryAt", false),
                        Filters.eq("nextRetryAt", null),
                        Filters.lte("nextRetryAt", now),
                    ),
                ),
                Updates.combine(
                    Updates.set("status", OutboxTaskStatus.PROCESSING.name),
                    Updates.set("updatedAt", now),
                ),
                FindOneAndUpdateOptions()
                    .sort(Sorts.orderBy(Sorts.ascending("priority"), Sorts.ascending("createdAt")))
                    .returnDocument(ReturnDocument.AFTER),
            )
        } ?: return null

        return doc.toTask()
    }

    override fun complete(task: OutboxTask) {
        val now = Instant.now()
        val archiveDoc = OutboxArchiveDocument().apply {
            id = task.id
            partition = task.partition
            eventType = task.eventType
            deduplicationKey = task.deduplicationKey
            payload = task.payload
            status = OutboxTaskStatus.DONE.name
            attempts = task.attempts
            createdAt = task.createdAt
            updatedAt = now
            nextRetryAt = task.nextRetryAt
            priority = task.priority.name
            lastError = task.lastError
            completedAt = now
        }
        outboxQueryMetrics.timed("outbox.complete.archive") {
            outboxArchiveDocumentRepository.persist(archiveDoc)
        }
        outboxQueryMetrics.timed("outbox.complete.delete") {
            outboxDocumentRepository.deleteById(task.id)
        }
    }

    override fun fail(task: OutboxTask, error: String, nextRetryAt: Instant?) {
        val now = Instant.now()
        if (nextRetryAt == null) {
            val archiveDoc = OutboxArchiveDocument().apply {
                id = task.id
                partition = task.partition
                eventType = task.eventType
                deduplicationKey = task.deduplicationKey
                payload = task.payload
                status = OutboxTaskStatus.FAILED.name
                attempts = task.attempts + 1
                createdAt = task.createdAt
                updatedAt = now
                this.nextRetryAt = null
                priority = task.priority.name
                lastError = error
                completedAt = now
            }
            outboxQueryMetrics.timed("outbox.fail.archive") {
                outboxArchiveDocumentRepository.persist(archiveDoc)
            }
            outboxQueryMetrics.timed("outbox.fail.delete") {
                outboxDocumentRepository.deleteById(task.id)
            }
        } else {
            val updates = mutableListOf(
                Updates.set("status", OutboxTaskStatus.PENDING.name),
                Updates.inc("attempts", 1),
                Updates.set("updatedAt", now),
                Updates.set("lastError", error),
                Updates.set("nextRetryAt", nextRetryAt),
            )
            outboxQueryMetrics.timed("outbox.fail") {
                outboxDocumentRepository.mongoCollection().updateOne(
                    Filters.eq("_id", task.id),
                    Updates.combine(updates),
                )
            }
        }
    }

    override fun reschedule(task: OutboxTask, nextRetryAt: Instant) {
        val now = Instant.now()
        outboxQueryMetrics.timed("outbox.reschedule") {
            outboxDocumentRepository.mongoCollection().updateOne(
                Filters.eq("_id", task.id),
                Updates.combine(
                    Updates.set("status", OutboxTaskStatus.PENDING.name),
                    Updates.set("updatedAt", now),
                    Updates.set("nextRetryAt", nextRetryAt),
                ),
            )
        }
    }

    override fun enqueue(
        partition: OutboxPartition,
        event: OutboxEvent,
        payload: String,
        priority: OutboxTaskPriority,
    ): Boolean {
        val deduplicationKey = event.deduplicationKey()
        val existing = outboxQueryMetrics.timed("outbox.enqueue.dedupCheck") {
            outboxDocumentRepository.mongoCollection().find(
                Filters.and(
                    Filters.eq("partition", partition.key),
                    Filters.eq("deduplicationKey", deduplicationKey),
                    Filters.`in`("status", OutboxTaskStatus.PENDING.name, OutboxTaskStatus.PROCESSING.name),
                ),
            ).first()
        }

        if (existing != null) {
            logger.debug { "Skipping duplicate outbox task: partition=${partition.key}, deduplicationKey=$deduplicationKey" }
            return false
        }

        val now = Instant.now()
        val doc = OutboxDocument().apply {
            id = UUID.randomUUID().toString()
            this.partition = partition.key
            this.eventType = event.key
            this.deduplicationKey = deduplicationKey
            this.payload = payload
            status = OutboxTaskStatus.PENDING.name
            attempts = 0
            createdAt = now
            updatedAt = now
            nextRetryAt = null
            this.priority = priority.name
            lastError = null
        }
        outboxQueryMetrics.timed("outbox.enqueue.insert") {
            outboxDocumentRepository.persist(doc)
        }
        return true
    }

    override fun pausePartition(partition: OutboxPartition, reason: String, pausedUntil: Instant) {
        outboxQueryMetrics.timed("outbox.pausePartition") {
            outboxPartitionDocumentRepository.mongoCollection().findOneAndUpdate(
                Filters.eq("_id", partition.key),
                Updates.combine(
                    Updates.set("status", OutboxPartitionStatus.PAUSED.name),
                    Updates.set("statusReason", reason),
                    Updates.set("pausedUntil", pausedUntil),
                ),
                FindOneAndUpdateOptions().upsert(true),
            )
        }
    }

    override fun activatePartition(partition: OutboxPartition) {
        outboxQueryMetrics.timed("outbox.activatePartition") {
            outboxPartitionDocumentRepository.mongoCollection().findOneAndUpdate(
                Filters.eq("_id", partition.key),
                Updates.combine(
                    Updates.set("status", OutboxPartitionStatus.ACTIVE.name),
                    Updates.unset("statusReason"),
                    Updates.unset("pausedUntil"),
                ),
                FindOneAndUpdateOptions().upsert(true),
            )
        }
    }

    override fun findPartition(partition: OutboxPartition): OutboxPartitionInfo? =
        outboxQueryMetrics.timed("outbox.findPartition") {
            outboxPartitionDocumentRepository.findById(partition.key)?.toInfo()
        }

    private fun OutboxPartitionDocument.toInfo() = OutboxPartitionInfo(
        key = partitionKey,
        status = status,
        statusReason = statusReason,
        pausedUntil = pausedUntil,
    )

    /** Resets all PROCESSING tasks back to PENDING. Should be called at application startup to recover tasks that were interrupted mid-processing. */
    override fun resetStaleProcessingTasks() {
        val now = Instant.now()
        val result = outboxQueryMetrics.timed("outbox.resetStaleProcessingTasks") {
            outboxDocumentRepository.mongoCollection().updateMany(
                Filters.eq("status", OutboxTaskStatus.PROCESSING.name),
                Updates.combine(
                    Updates.set("status", OutboxTaskStatus.PENDING.name),
                    Updates.set("nextRetryAt", now),
                    Updates.set("updatedAt", now),
                ),
            )
        }
        if (result.modifiedCount > 0) {
            logger.info { "Reset ${result.modifiedCount} stale PROCESSING tasks back to PENDING" }
        }
    }

    override fun countByPartition(partition: OutboxPartition): Long =
        outboxQueryMetrics.timed("outbox.countByPartition") {
            outboxDocumentRepository.count("partition = ?1", partition.key)
        }

    override fun migratePartition(fromKey: String, toPartition: OutboxPartition): Long {
        val now = Instant.now()
        val result = outboxQueryMetrics.timed("outbox.migratePartition") {
            outboxDocumentRepository.mongoCollection().updateMany(
                Filters.eq("partition", fromKey),
                Updates.combine(
                    Updates.set("partition", toPartition.key),
                    Updates.set("updatedAt", now),
                ),
            )
        }
        return result.modifiedCount
    }

    fun deleteArchiveEntriesOlderThan(cutoff: Instant): Long {
        val result = outboxQueryMetrics.timed("outbox_archive.deleteEntriesOlderThan") {
            outboxArchiveDocumentRepository.mongoCollection().deleteMany(
                Filters.lt("completedAt", cutoff),
            )
        }
        return result.deletedCount
    }

    override fun archiveFailedTasks(): Long {
        val now = Instant.now()
        val failedDocs = outboxQueryMetrics.timed("outbox.archiveFailedTasks.find") {
            outboxDocumentRepository.list("status = ?1", OutboxTaskStatus.FAILED.name)
        }
        if (failedDocs.isEmpty()) return 0L
        var count = 0L
        for (doc in failedDocs) {
            val archiveDoc = OutboxArchiveDocument().apply {
                id = doc.id
                partition = doc.partition
                eventType = doc.eventType
                deduplicationKey = doc.deduplicationKey
                payload = doc.payload
                status = OutboxTaskStatus.FAILED.name
                attempts = doc.attempts
                createdAt = doc.createdAt
                updatedAt = now
                nextRetryAt = null
                priority = doc.priority
                lastError = doc.lastError
                completedAt = now
            }
            outboxQueryMetrics.timed("outbox.archiveFailedTasks.archive") {
                outboxArchiveDocumentRepository.mongoCollection().replaceOne(
                    Filters.eq("_id", doc.id),
                    archiveDoc,
                    ReplaceOptions().upsert(true),
                )
            }
            outboxQueryMetrics.timed("outbox.archiveFailedTasks.delete") {
                outboxDocumentRepository.deleteById(doc.id)
            }
            count++
        }
        logger.info { "Archived $count failed outbox tasks" }
        return count
    }

    private fun OutboxDocument.toTask() = OutboxTask(
        id = id,
        partition = partition,
        eventType = eventType,
        payload = payload,
        deduplicationKey = deduplicationKey,
        status = OutboxTaskStatus.valueOf(status),
        attempts = attempts,
        createdAt = createdAt,
        updatedAt = updatedAt,
        nextRetryAt = nextRetryAt,
        priority = OutboxTaskPriority.valueOf(priority),
        lastError = lastError,
    )

    companion object : KLogging()
}
