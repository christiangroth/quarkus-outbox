package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import io.quarkus.mongodb.panache.common.MongoEntity
import org.bson.codecs.pojo.annotations.BsonId
import java.time.Instant

@MongoEntity(collection = "outbox")
class Task {

  @BsonId
  lateinit var id: String
  lateinit var partition: String
  lateinit var eventType: String
  lateinit var deduplicationKey: String
  lateinit var payload: String
  lateinit var status: String
  var attempts: Int = 0
  lateinit var createdAt: Instant
  lateinit var updatedAt: Instant
  var nextRetryAt: Instant? = null
  lateinit var priority: String
  var priorityOrder: Int = 1
  var lastError: String? = null
}

@MongoEntity(collection = "outbox_archive")
class ArchivedTask {

  @BsonId
  lateinit var id: String
  lateinit var partition: String
  lateinit var eventType: String
  lateinit var deduplicationKey: String
  lateinit var payload: String
  lateinit var status: String
  var attempts: Int = 0
  lateinit var createdAt: Instant
  lateinit var updatedAt: Instant
  var nextRetryAt: Instant? = null
  lateinit var priority: String
  var lastError: String? = null
  lateinit var completedAt: Instant
}

@MongoEntity(collection = "outbox_partitions")
class Partition {

  @BsonId
  lateinit var partitionKey: String
  lateinit var status: String
  var statusReason: String? = null
  var pausedUntil: Instant? = null
}
