package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import com.mongodb.client.model.Filters
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.model.Updates
import de.chrgroth.quarkus.outbox.adapter.out.mongodb.Partition
import de.chrgroth.quarkus.outbox.domain.OutboxPartition
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

  override fun findPartition(partitionKey: String): OutboxPartitionInfo? =
    metricsRecorder.timed("outbox.partition.find") {
      findById(partitionKey)?.toInfo()
    }

  override fun findOrCreate(partition: OutboxPartition): OutboxPartitionInfo {
    val doc = metricsRecorder.timed("outbox.partition.findOrCreate") {
      mongoCollection().findOneAndUpdate(
        Filters.eq("_id", partition.key),
        Updates.setOnInsert("status", OutboxPartitionStatus.ACTIVE.name),
        FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER),
      )
    }
    return doc!!.toInfo()
  }

  override fun pause(partition: OutboxPartition, reason: String, pausedUntil: Instant) {
    metricsRecorder.timed("outbox.partition.pause") {
      mongoCollection().findOneAndUpdate(
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

  override fun activate(partition: OutboxPartition) {
    metricsRecorder.timed("outbox.partition.activate") {
      mongoCollection().findOneAndUpdate(
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

  private fun Partition.toInfo() = OutboxPartitionInfo(
    key = partitionKey,
    status = status,
    statusReason = statusReason,
    pausedUntil = pausedUntil,
  )
}

