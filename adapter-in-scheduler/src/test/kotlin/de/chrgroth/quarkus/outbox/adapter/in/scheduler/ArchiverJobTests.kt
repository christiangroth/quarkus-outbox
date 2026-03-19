package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Instant

class ArchiverJobTests {

  private val archiverPort: ArchiverPort = mockk(relaxed = true)

  private val job = ArchiverJob(archiverPort, retentionDays = 30L)

  @Test
  fun `run archives failed tasks`() {
    every { archiverPort.archiveFailedTasks() } returns 3L
    every { archiverPort.deleteEntriesOlderThan(any()) } returns 2L

    job.run()

    verify { archiverPort.archiveFailedTasks() }
  }

  @Test
  fun `run deletes archive entries older than retention period`() {
    every { archiverPort.archiveFailedTasks() } returns 0L
    every { archiverPort.deleteEntriesOlderThan(any()) } returns 5L

    job.run()

    verify {
      archiverPort.deleteEntriesOlderThan(
        match { cutoff ->
          val expectedCutoff = Instant.now().minusSeconds(30 * 24 * 3600)
          cutoff.isAfter(expectedCutoff.minusSeconds(5)) && cutoff.isBefore(expectedCutoff.plusSeconds(5))
        }
      )
    }
  }
}
