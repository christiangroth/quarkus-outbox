package de.chrgroth.quarkus.outbox.domain.port.`in`

import java.time.Instant

interface ArchiverPort {

  fun archiveFailedTasks(): Long

  fun deleteEntriesOlderThan(cutoff: Instant): Long
}
