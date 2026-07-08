package de.chrgroth.quarkus.outbox.domain.port.`in`

interface PartitionCountReconciliationPort {

  /**
   * Recomputes per-event-type task counts from the source of truth and corrects any partition
   * whose persisted counters have drifted. Returns the number of partitions that were corrected.
   */
  fun reconcileEventTypeCounts(): Long
}
