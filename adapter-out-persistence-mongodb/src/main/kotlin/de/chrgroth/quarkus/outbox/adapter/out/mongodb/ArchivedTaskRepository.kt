package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import io.quarkus.mongodb.panache.kotlin.PanacheMongoRepositoryBase
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class ArchivedTaskRepository : PanacheMongoRepositoryBase<ArchivedTask, String>
