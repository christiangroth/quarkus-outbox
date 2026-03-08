package de.chrgroth.outbox

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class OutboxArchiveCleanupJobTests {

    private val repository: MongoOutboxRepository = mockk()

    @Test
    fun `run deletes archive entries older than retention period`() {
        val job = OutboxArchiveCleanupJob(repository, retentionDays = 365)
        val cutoffSlot = slot<Instant>()
        every { repository.deleteArchiveEntriesOlderThan(capture(cutoffSlot)) } returns 3

        val before = Instant.now().minus(365, ChronoUnit.DAYS)
        job.run()
        val after = Instant.now().minus(365, ChronoUnit.DAYS)

        verify { repository.deleteArchiveEntriesOlderThan(any()) }
        assertThat(cutoffSlot.captured).isBetween(before, after)
    }

    @Test
    fun `run respects configured retention days`() {
        val job = OutboxArchiveCleanupJob(repository, retentionDays = 30)
        val cutoffSlot = slot<Instant>()
        every { repository.deleteArchiveEntriesOlderThan(capture(cutoffSlot)) } returns 0

        val before = Instant.now().minus(30, ChronoUnit.DAYS)
        job.run()
        val after = Instant.now().minus(30, ChronoUnit.DAYS)

        assertThat(cutoffSlot.captured).isBetween(before, after)
    }
}
