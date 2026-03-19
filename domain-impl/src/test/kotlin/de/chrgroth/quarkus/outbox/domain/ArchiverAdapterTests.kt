package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class ArchiverAdapterTests {

  private val taskPort: TaskRepositoryPort = mockk()
  private val archivePort: ArchivedTaskRepositoryPort = mockk()

  private val adapter = ArchiverAdapter(taskPort, archivePort)

  private fun task() = OutboxTask(
    id = "task-1",
    partition = "test-partition",
    eventType = "TEST_EVENT",
    payload = """{"foo":"bar"}""",
    deduplicationKey = "dedup-1",
    status = OutboxTaskStatus.FAILED,
    attempts = 3,
    createdAt = Instant.now(),
    updatedAt = Instant.now(),
    nextRetryAt = null,
    priority = OutboxTaskPriority.NORMAL,
    lastError = "some error",
  )

  @Test
  fun `archiveFailedTasks returns 0 when no failed tasks`() {
    every { taskPort.listFailed() } returns emptyList()

    assertThat(adapter.archiveFailedTasks()).isEqualTo(0L)
    verify(exactly = 0) { archivePort.upsertFailed(any()) }
  }

  @Test
  fun `archiveFailedTasks upserts and deletes each failed task`() {
    val task1 = task().copy(id = "task-1")
    val task2 = task().copy(id = "task-2")
    every { taskPort.listFailed() } returns listOf(task1, task2)
    every { archivePort.upsertFailed(any()) } just runs
    every { taskPort.delete(any()) } just runs

    assertThat(adapter.archiveFailedTasks()).isEqualTo(2L)
    verify { archivePort.upsertFailed(task1) }
    verify { archivePort.upsertFailed(task2) }
    verify { taskPort.delete(task1) }
    verify { taskPort.delete(task2) }
  }

  @Test
  fun `deleteEntriesOlderThan delegates to archivePort`() {
    val cutoff = Instant.now().minusSeconds(3600)
    every { archivePort.deleteEntriesOlderThan(cutoff) } returns 5L

    assertThat(adapter.deleteEntriesOlderThan(cutoff)).isEqualTo(5L)
    verify { archivePort.deleteEntriesOlderThan(cutoff) }
  }
}
