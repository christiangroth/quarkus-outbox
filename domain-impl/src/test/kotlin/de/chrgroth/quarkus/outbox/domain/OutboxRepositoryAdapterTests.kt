package de.chrgroth.quarkus.outbox.domain

import de.chrgroth.quarkus.outbox.domain.port.out.ArchivedTaskRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.PartitionRepositoryPort
import de.chrgroth.quarkus.outbox.domain.port.out.TaskRepositoryPort
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class OutboxRepositoryAdapterTests {

  private val taskPort: TaskRepositoryPort = mockk()
  private val archivePort: ArchivedTaskRepositoryPort = mockk()
  private val partitionPort: PartitionRepositoryPort = mockk()

  private val adapter = OutboxRepositoryAdapter(taskPort, archivePort, partitionPort)

  private val partition = object : OutboxPartition {
    override val key = "test-partition"
  }

  private fun task() = OutboxTask(
    id = "task-1",
    partition = partition.key,
    eventType = "TEST_EVENT",
    payload = """{"foo":"bar"}""",
    deduplicationKey = "dedup-1",
    status = OutboxTaskStatus.PROCESSING,
    attempts = 0,
    createdAt = Instant.now(),
    updatedAt = Instant.now(),
    nextRetryAt = null,
    priority = OutboxTaskPriority.NORMAL,
    lastError = null,
  )

  // --- claim ---

  @Test
  fun `claim returns null when partition is paused`() {
    every { partitionPort.findPartition(partition.key) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.PAUSED.name,
      statusReason = "manual",
      pausedUntil = null,
    )

    val result = adapter.claim(partition)

    assertThat(result).isNull()
    verify(exactly = 0) { taskPort.claim(any()) }
  }

  @Test
  fun `claim delegates to taskPort when partition is active`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns OutboxPartitionInfo(
      key = partition.key,
      status = OutboxPartitionStatus.ACTIVE.name,
      statusReason = null,
      pausedUntil = null,
    )
    every { taskPort.claim(partition) } returns task

    val result = adapter.claim(partition)

    assertThat(result).isEqualTo(task)
  }

  @Test
  fun `claim delegates to taskPort when partition is not found`() {
    val task = task()
    every { partitionPort.findPartition(partition.key) } returns null
    every { taskPort.claim(partition) } returns task

    val result = adapter.claim(partition)

    assertThat(result).isEqualTo(task)
  }

  // --- complete ---

  @Test
  fun `complete archives task and deletes it`() {
    val task = task()
    every { archivePort.append(task) } just runs
    every { taskPort.delete(task) } just runs

    adapter.complete(task)

    verify { archivePort.append(task) }
    verify { taskPort.delete(task) }
  }

  // --- fail ---

  @Test
  fun `fail with null nextRetryAt appends to archive and deletes task`() {
    val task = task()
    every { archivePort.appendFailed(task, "error") } just runs
    every { taskPort.delete(task) } just runs

    adapter.fail(task, "error", null)

    verify { archivePort.appendFailed(task, "error") }
    verify { taskPort.delete(task) }
    verify(exactly = 0) { taskPort.scheduleRetry(any(), any(), any()) }
  }

  @Test
  fun `fail with nextRetryAt schedules retry`() {
    val task = task()
    val nextRetryAt = Instant.now().plusSeconds(30)
    every { taskPort.scheduleRetry(task, "error", nextRetryAt) } just runs

    adapter.fail(task, "error", nextRetryAt)

    verify { taskPort.scheduleRetry(task, "error", nextRetryAt) }
    verify(exactly = 0) { archivePort.appendFailed(any(), any()) }
  }

  // --- archiveFailedTasks ---

  @Test
  fun `archiveFailedTasks returns 0 when no failed tasks`() {
    every { taskPort.listFailed() } returns emptyList()

    val count = adapter.archiveFailedTasks()

    assertThat(count).isEqualTo(0L)
    verify(exactly = 0) { archivePort.upsertFailed(any()) }
  }

  @Test
  fun `archiveFailedTasks upserts and deletes each failed task`() {
    val task1 = task().copy(id = "task-1")
    val task2 = task().copy(id = "task-2")
    every { taskPort.listFailed() } returns listOf(task1, task2)
    every { archivePort.upsertFailed(any()) } just runs
    every { taskPort.delete(any()) } just runs

    val count = adapter.archiveFailedTasks()

    assertThat(count).isEqualTo(2L)
    verify { archivePort.upsertFailed(task1) }
    verify { archivePort.upsertFailed(task2) }
    verify { taskPort.delete(task1) }
    verify { taskPort.delete(task2) }
  }

  // --- deleteArchiveEntriesOlderThan ---

  @Test
  fun `deleteArchiveEntriesOlderThan delegates to archivePort`() {
    val cutoff = Instant.now()
    every { archivePort.deleteEntriesOlderThan(cutoff) } returns 5L

    val count = adapter.deleteArchiveEntriesOlderThan(cutoff)

    assertThat(count).isEqualTo(5L)
  }

  // --- findPartition ---

  @Test
  fun `findPartition delegates to partitionPort`() {
    val info = OutboxPartitionInfo(partition.key, OutboxPartitionStatus.ACTIVE.name, null, null)
    every { partitionPort.findPartition(partition.key) } returns info

    val result = adapter.findPartition(partition)

    assertThat(result).isEqualTo(info)
  }

  // --- findOrCreatePartition ---

  @Test
  fun `findOrCreatePartition delegates to partitionPort`() {
    val info = OutboxPartitionInfo(partition.key, OutboxPartitionStatus.ACTIVE.name, null, null)
    every { partitionPort.findOrCreate(partition) } returns info

    val result = adapter.findOrCreatePartition(partition)

    assertThat(result).isEqualTo(info)
  }
}
