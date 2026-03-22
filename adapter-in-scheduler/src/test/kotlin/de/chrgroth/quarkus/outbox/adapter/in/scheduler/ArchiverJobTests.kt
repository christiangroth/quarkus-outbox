package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.ArchiverPort
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
  private val meterRegistry = SimpleMeterRegistry()

  private fun job(enabled: Boolean = true, retentionDays: Long = 30L) =
    ArchiverJob(archiverPort, meterRegistry, enabled, retentionDays)

  @Test
  fun `run deletes entries older than retention days`() {
    val retentionDays = 30L
    val capturedCutoff = slot<Instant>()
    every { archiverPort.deleteOlderThan(capture(capturedCutoff)) } returns 5L

    val before = Instant.now()
    job(retentionDays = retentionDays).run()
    val after = Instant.now()

    verify { archiverPort.deleteOlderThan(any()) }
    assertThat(capturedCutoff.captured).isBefore(before.minus(retentionDays - 1, ChronoUnit.DAYS))
    assertThat(capturedCutoff.captured).isAfter(after.minus(retentionDays + 1, ChronoUnit.DAYS))
  }

  @Test
  fun `run returns deletion count from archive port`() {
    every { archiverPort.deleteOlderThan(any()) } returns 12L

    job(retentionDays = 7L).run()

    verify { archiverPort.deleteOlderThan(any()) }
  }

  @Test
  fun `run works correctly when no entries are deleted`() {
    every { archiverPort.deleteOlderThan(any()) } returns 0L

    job(retentionDays = 90L).run()

    verify(exactly = 1) { archiverPort.deleteOlderThan(any()) }
  }

  @Test
  fun `test returns true when disabled so scheduler skips run`() {
    assertThat(job(enabled = false).test(mockk())).isTrue()
  }

  @Test
  fun `test returns false when enabled so scheduler runs`() {
    assertThat(job(enabled = true).test(mockk())).isFalse()
  }

  @Test
  fun `run records timer when executed`() {
    every { archiverPort.deleteOlderThan(any()) } returns 3L

    job().run()

    assertThat(meterRegistry.find("outbox.archive.cleanup.duration").timer()?.count()).isEqualTo(1L)
  }

  @Test
  fun `run increments deletion counter by deletion count`() {
    every { archiverPort.deleteOlderThan(any()) } returns 7L

    job().run()

    assertThat(meterRegistry.counter("outbox.archive.cleanup.deleted").count()).isEqualTo(7.0)
  }

  @Test
  fun `run accumulates deletion counter across multiple runs`() {
    every { archiverPort.deleteOlderThan(any()) } returns 3L

    val j = job()
    j.run()
    j.run()

    assertThat(meterRegistry.counter("outbox.archive.cleanup.deleted").count()).isEqualTo(6.0)
  }
}
