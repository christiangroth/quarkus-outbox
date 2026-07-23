package de.chrgroth.quarkus.outbox.domain.startup

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.quarkus.runtime.StartupEvent
import org.junit.jupiter.api.Test

class ArchiveCleanupStarterTests {

  private val archivePort: ArchivedTaskRepositoryPort = mockk()

  private val startupEvent = StartupEvent()

  private fun starter(archiveEnabled: Boolean = true) = ArchiveCleanupStarter(archivePort, archiveEnabled)

  @Test
  fun `onStart does nothing when archive is enabled`() {
    starter(archiveEnabled = true).onStart(startupEvent)

    verify(exactly = 0) { archivePort.count() }
    verify(exactly = 0) { archivePort.deleteAll() }
  }

  @Test
  fun `onStart does nothing when archive is disabled but already empty`() {
    every { archivePort.count() } returns 0L

    starter(archiveEnabled = false).onStart(startupEvent)

    verify { archivePort.count() }
    verify(exactly = 0) { archivePort.deleteAll() }
  }

  @Test
  fun `onStart clears archive collection when archive is disabled and not empty`() {
    every { archivePort.count() } returns 42L
    every { archivePort.deleteAll() } returns 42L

    starter(archiveEnabled = false).onStart(startupEvent)

    verify { archivePort.count() }
    verify { archivePort.deleteAll() }
  }
}
