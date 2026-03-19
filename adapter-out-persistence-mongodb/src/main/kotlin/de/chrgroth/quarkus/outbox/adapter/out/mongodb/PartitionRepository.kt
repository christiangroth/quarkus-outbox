package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import de.chrgroth.quarkus.outbox.adapter.out.mongodb.documents.Partition
import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class PartitionRepository : PanacheMongoRepositoryBase<Partition, String>
