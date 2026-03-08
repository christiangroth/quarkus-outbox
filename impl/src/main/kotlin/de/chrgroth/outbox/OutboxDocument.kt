package de.chrgroth.outbox

import io.quarkus.mongodb.panache.common.MongoEntity
import org.bson.codecs.pojo.annotations.BsonId
import java.time.Instant

@MongoEntity(collection = "outbox")
class OutboxDocument {

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
}
