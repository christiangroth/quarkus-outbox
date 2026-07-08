package de.chrgroth.quarkus.outbox.adapter.`in`.scheduler

import de.chrgroth.quarkus.outbox.domain.port.`in`.PartitionCountReconciliationPort
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PartitionCountReconciliationJobTests {

  private val reconciliationPort: PartitionCountReconciliationPort = mockk()
  private val meterRegistry = SimpleMeterRegistry()

  private fun job(enabled: Boolean = true) =
    PartitionCountReconciliationJob(reconciliationPort, meterRegistry, enabled)

  @Test
  fun `run delegates to reconciliation port`() {
    every { reconciliationPort.reconcileEventTypeCounts() } returns 2L

    job().run()

    verify { reconciliationPort.reconcileEventTypeCounts() }
  }

  @Test
  fun `run works correctly when nothing is drifted`() {
    every { reconciliationPort.reconcileEventTypeCounts() } returns 0L

    job().run()

    verify(exactly = 1) { reconciliationPort.reconcileEventTypeCounts() }
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
    every { reconciliationPort.reconcileEventTypeCounts() } returns 1L

    job().run()

    assertThat(meterRegistry.find("outbox.reconciliation.duration").timer()?.count()).isEqualTo(1L)
  }

  @Test
  fun `run increments corrected counter by corrected partition count`() {
    every { reconciliationPort.reconcileEventTypeCounts() } returns 4L

    job().run()

    assertThat(meterRegistry.counter("outbox.reconciliation.corrected").count()).isEqualTo(4.0)
  }

  @Test
  fun `run accumulates corrected counter across multiple runs`() {
    every { reconciliationPort.reconcileEventTypeCounts() } returns 2L

    val j = job()
    j.run()
    j.run()

    assertThat(meterRegistry.counter("outbox.reconciliation.corrected").count()).isEqualTo(4.0)
  }
}
