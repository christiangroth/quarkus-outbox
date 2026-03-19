package de.chrgroth.quarkus.outbox.domain.port.`in`

import java.time.Instant

interface ArchiverPort {

  fun deleteOlderThan(cutoff: Instant): Long
}
