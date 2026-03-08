package de.chrgroth.outbox

import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class OutboxPartitionDocumentRepository : PanacheMongoRepositoryBase<OutboxPartitionDocument, String>
