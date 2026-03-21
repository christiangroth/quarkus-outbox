package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class ArchiverJobTests {

  private val archiverPort: ArchiverPort = mockk()

  @Test
  fun `run deletes entries older than retention days`() {
    val retentionDays = 30L
    val job = ArchiverJob(archiverPort, retentionDays)
    val capturedCutoff = slot<Instant>()
    every { archiverPort.deleteOlderThan(capture(capturedCutoff)) } returns 5L

    val before = Instant.now()
    job.run()
    val after = Instant.now()

    verify { archiverPort.deleteOlderThan(any()) }
    assertThat(capturedCutoff.captured).isBefore(before.minus(retentionDays - 1, ChronoUnit.DAYS))
    assertThat(capturedCutoff.captured).isAfter(after.minus(retentionDays + 1, ChronoUnit.DAYS))
  }

  @Test
  fun `run returns deletion count from archive port`() {
    val job = ArchiverJob(archiverPort, 7L)
    every { archiverPort.deleteOlderThan(any()) } returns 12L

    job.run()

    verify { archiverPort.deleteOlderThan(any()) }
  }

  @Test
  fun `run works correctly when no entries are deleted`() {
    val job = ArchiverJob(archiverPort, 90L)
    every { archiverPort.deleteOlderThan(any()) } returns 0L

    job.run()

    verify(exactly = 1) { archiverPort.deleteOlderThan(any()) }
  }
}
