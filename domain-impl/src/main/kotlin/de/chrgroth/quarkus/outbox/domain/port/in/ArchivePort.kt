package de.chrgroth.quarkus.outbox.domain.port.`in`

interface ArchivePort {

  fun archiveFailedTasks(): Long
}
