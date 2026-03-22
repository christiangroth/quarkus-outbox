package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class ArchiverAdapterTests {

  private val archivePort: ArchivedTaskRepositoryPort = mockk()
  private val meterRegistry = SimpleMeterRegistry()
  private val adapter = ArchiverAdapter(archivePort, meterRegistry)

  @Test
  fun `deleteOlderThan delegates to archive port and returns count`() {
    val cutoff = Instant.now().minusSeconds(86400)
    every { archivePort.deleteOlderThan(cutoff) } returns 7L

    val result = adapter.deleteOlderThan(cutoff)

    assertThat(result).isEqualTo(7L)
    verify { archivePort.deleteOlderThan(cutoff) }
  }

  @Test
  fun `deleteOlderThan returns zero when nothing deleted`() {
    val cutoff = Instant.now()
    every { archivePort.deleteOlderThan(cutoff) } returns 0L

    val result = adapter.deleteOlderThan(cutoff)

    assertThat(result).isEqualTo(0L)
  }

  @Test
  fun `gauge reflects current archive task count`() {
    every { archivePort.count() } returns 42L

    val gauge = meterRegistry.find("outbox.archive.size").gauge()
    assertThat(gauge).isNotNull()
    assertThat(gauge!!.value()).isEqualTo(42.0)
  }

  @Test
  fun `gauge reflects updated archive task count`() {
    every { archivePort.count() } returnsMany listOf(10L, 15L)

    val gauge = meterRegistry.find("outbox.archive.size").gauge()!!
    assertThat(gauge.value()).isEqualTo(10.0)
    assertThat(gauge.value()).isEqualTo(15.0)
  }
}
