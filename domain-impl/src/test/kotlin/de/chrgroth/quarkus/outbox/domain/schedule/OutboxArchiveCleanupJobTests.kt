package de.chrgroth.quarkus.outbox.domain.schedule

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class OutboxArchiveCleanupJobTests {

  private val archivePort: ArchivedTaskRepositoryPort = mockk()

  @Test
  fun `run deletes archive entries older than retention period`() {
    val job = OutboxArchiveCleanupJob(archivePort, retentionDays = 365)
    val cutoffSlot = slot<Instant>()
    every { archivePort.deleteEntriesOlderThan(capture(cutoffSlot)) } returns 3

    val before = Instant.now().minus(365, ChronoUnit.DAYS)
    job.run()
    val after = Instant.now().minus(365, ChronoUnit.DAYS)

    verify { archivePort.deleteEntriesOlderThan(any()) }
    assertThat(cutoffSlot.captured).isBetween(before, after)
  }

  @Test
  fun `run respects configured retention days`() {
    val job = OutboxArchiveCleanupJob(archivePort, retentionDays = 30)
    val cutoffSlot = slot<Instant>()
    every { archivePort.deleteEntriesOlderThan(capture(cutoffSlot)) } returns 0

    val before = Instant.now().minus(30, ChronoUnit.DAYS)
    job.run()
    val after = Instant.now().minus(30, ChronoUnit.DAYS)

    assertThat(cutoffSlot.captured).isBetween(before, after)
  }
}
