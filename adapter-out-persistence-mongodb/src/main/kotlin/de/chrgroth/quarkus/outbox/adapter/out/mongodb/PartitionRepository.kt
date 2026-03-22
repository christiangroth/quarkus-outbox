package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.domain.ApplicationOutboxPartition
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionInfo
import de.chrgroth.quarkus.outbox.domain.OutboxPartitionStatus
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class PartitionRepository : PartitionRepositoryPort, PanacheMongoRepositoryBase<Partition, String> {

  @Inject
  lateinit var metricsRecorder: MetricsRecorder

  override fun findOrCreate(partition: ApplicationOutboxPartition): OutboxPartitionInfo {
    val doc = metricsRecorder.timed("outbox.partition.findOrCreate") {
      mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
    return doc!!.toInfo()
  }

  override fun findAllPartitions(): List<OutboxPartitionInfo> =
    metricsRecorder.timed("outbox.partition.findAll") {
      listAll().map { it.toInfo() }
    }

  override fun pause(partition: ApplicationOutboxPartition, reason: String?, pausedUntil: Instant?) {
    val updates = mutableListOf(Updates.set("status", OutboxPartitionStatus.PAUSED.name))
    if (reason != null) updates.add(Updates.set("statusReason", reason)) else updates.add(Updates.unset("statusReason"))
    if (pausedUntil != null) updates.add(Updates.set("pausedUntil", pausedUntil)) else updates.add(Updates.unset("pausedUntil"))
    metricsRecorder.timed("outbox.partition.pause") {
      mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.combine(updates),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
  }

  override fun resume(partition: ApplicationOutboxPartition) {
    metricsRecorder.timed("outbox.partition.resume") {
      mongoCollection().findOneAndUpdate(
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

  private fun Partition.toInfo() = OutboxPartitionInfo(
    key = partitionKey,
    status = OutboxPartitionStatus.valueOf(status),
    statusReason = statusReason,
    pausedUntil = pausedUntil,
  )
}

