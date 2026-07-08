package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class PartitionRepository : PartitionRepositoryPort {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  @Inject
  lateinit var repository: PartitionMongoRepository

  override fun findOrCreate(partition: ApplicationOutboxPartition): OutboxPartitionInfo {
    val doc = metricsRecorder.timed("outbox.partition.findOrCreate") {
      repository.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
    return doc!!.toInfo()
  }

  override fun findAllPartitions(): List<OutboxPartitionInfo> =
    metricsRecorder.timed("outbox.partition.findAll") {
      repository.listAll().map { it.toInfo() }
    }

  override fun pause(partition: ApplicationOutboxPartition, reason: String?, pausedUntil: Instant?) {
    val updates = mutableListOf(Updates.set("status", OutboxPartitionStatus.PAUSED.name))
    if (reason != null) updates.add(Updates.set("statusReason", reason)) else updates.add(Updates.unset("statusReason"))
    if (pausedUntil != null) updates.add(Updates.set("pausedUntil", pausedUntil)) else updates.add(Updates.unset("pausedUntil"))
    metricsRecorder.timed("outbox.partition.pause") {
      repository.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(updates),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
  }

  override fun resume(partition: ApplicationOutboxPartition) {
    metricsRecorder.timed("outbox.partition.resume") {
      repository.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(
          Updates.set("status", OutboxPartitionStatus.ACTIVE.name),
          Updates.unset("statusReason"),
          Updates.unset("pausedUntil"),
        ),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
  }

  override fun incrementEventTypeCount(partition: ApplicationOutboxPartition, eventType: String) =
    adjustEventTypeCount(partition, eventType, 1L)

  override fun decrementEventTypeCount(partition: ApplicationOutboxPartition, eventType: String) =
    adjustEventTypeCount(partition, eventType, -1L)

  override fun replaceEventTypeCounts(partition: ApplicationOutboxPartition, counts: Map<String, Long>) {
    metricsRecorder.timed("outbox.partition.replaceEventTypeCounts") {
      repository.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(
          Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
          Updates.set("eventTypeCounts", counts.mapKeys { encodeFieldKey(it.key) }),
        ),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
  }

  private fun adjustEventTypeCount(partition: ApplicationOutboxPartition, eventType: String, delta: Long) {
    val fieldKey = encodeFieldKey(eventType)
    val updated = metricsRecorder.timed("outbox.partition.adjustEventTypeCount") {
      repository.mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(
          Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
          Updates.inc("eventTypeCounts.$fieldKey", delta),
        ),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }

    // remove keys that dropped to zero (or below) so the document does not grow unbounded with stale entries
    if (delta < 0 && (updated?.eventTypeCounts?.get(fieldKey) ?: 0L) <= 0L) {
      metricsRecorder.timed("outbox.partition.pruneEventTypeCount") {
        repository.mongoCollection().updateOne(
          Filters.eq("_id", partition.key),
          Updates.unset("eventTypeCounts.$fieldKey"),
        )
      }
    }
  }

  /**
   * MongoDB interprets `.` as a nested-field separator and a leading `$` as an update operator when used
   * in a dynamic field path (e.g. `eventTypeCounts.$eventType`). Event type keys are free-form
   * application-defined strings (e.g. fully qualified class names), so they must be encoded before use
   * as a map field name to avoid corrupting the document structure.
   */
  private fun encodeFieldKey(key: String) = key
    .replace("%", "%25")
    .replace(".", "%2E")
    .replace("$", "%24")

  private fun decodeFieldKey(key: String) = key
    .replace("%24", "$")
    .replace("%2E", ".")
    .replace("%25", "%")

  private fun Partition.toInfo() = OutboxPartitionInfo(
    key = partitionKey,
    status = OutboxPartitionStatus.valueOf(status),
    statusReason = statusReason,
    pausedUntil = pausedUntil,
    eventCount = eventTypeCounts.values.sum(),
    eventPerTypeCount = eventTypeCounts
      .mapKeys { decodeFieldKey(it.key) }
      .filterValues { it > 0 }
      .ifEmpty { null },
  )
}

