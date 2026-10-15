package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class MetricsRecorderTests {

  private val registry = SimpleMeterRegistry()

  @Test
  fun `records the shared mongodb query timer tagged with the operation`() {
    val recorder = MetricsRecorder(registry, Long.MAX_VALUE)

    val result = recorder.timed("outbox.task.claim") { "value" }

    assertThat(result).isEqualTo("value")
    val timer = registry.find("mongodb.query").tag("operation", "outbox.task.claim").timer()
    assertThat(timer).isNotNull
    assertThat(timer!!.count()).isEqualTo(1)
    assertThat(registry.find("mongodb.slow.queries").counter()).isNull()
  }

  @Test
  fun `counts slow queries with the shared slow query counter`() {
    val recorder = MetricsRecorder(registry, 0)

    recorder.timed("outbox.task.insert") { }

    val counter = registry.find("mongodb.slow.queries").tag("operation", "outbox.task.insert").counter()
    assertThat(counter).isNotNull
    assertThat(counter!!.count()).isEqualTo(1.0)
  }

  @Test
  fun `records the timing even when the block throws`() {
    val recorder = MetricsRecorder(registry, Long.MAX_VALUE)

    assertThatThrownBy { recorder.timed<Unit>("outbox.task.delete") { error("boom") } }
      .isInstanceOf(IllegalStateException::class.java)
      .hasMessage("boom")

    val timer = registry.find("mongodb.query").tag("operation", "outbox.task.delete").timer()
    assertThat(timer).isNotNull
    assertThat(timer!!.count()).isEqualTo(1)
  }

  @Test
  fun `does not register the legacy outbox specific metric names`() {
    val recorder = MetricsRecorder(registry, 0)

    recorder.timed("outbox.archive.count") { 0L }

    assertThat(registry.find("outbox.mongodb.query.duration").meters()).isEmpty()
    assertThat(registry.find("outbox.mongodb.query.slow").meters()).isEmpty()
  }
}
