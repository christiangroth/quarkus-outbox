package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class ArchiverAdapterTests {

  private val archivePort: ArchivedTaskRepositoryPort = mockk()
  private val adapter = ArchiverAdapter(archivePort)

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
}
