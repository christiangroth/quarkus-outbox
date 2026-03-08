package de.chrgroth.outbox

import io.quarkus.mongodb.panache.common.MongoEntity
import org.bson.codecs.pojo.annotations.BsonId
import java.time.Instant

@MongoEntity(collection = "outbox_partitions")
class OutboxPartitionDocument {

    @BsonId
    lateinit var partitionKey: String
    lateinit var status: String
    var statusReason: String? = null
    var pausedUntil: Instant? = null
}
