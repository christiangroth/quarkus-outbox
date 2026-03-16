package de.chrgroth.quarkus.outbox.domain

import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class OutboxPartitionDocumentRepository : PanacheMongoRepositoryBase<OutboxPartitionDocument, String>
