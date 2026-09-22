package de.chrgroth.quarkus.outbox.adapter.out.mongodb

/**
 * Computes the worker bucket a task with a given `groupId` is routed to, for a partition
 * running with [workerCount] concurrent workers.
 *
 * Tasks without a `groupId` are always routed to bucket `0`, so they are claimed exclusively
 * by worker `0` and therefore keep today's total ordering guarantee across the whole
 * partition even when [workerCount] is greater than `1`.
 *
 * Tasks with a `groupId` are routed via a consistent hash (`hash(groupId) % workerCount`),
 * so the same `groupId` always maps to the same bucket for a given [workerCount] and is
 * therefore always claimed by the same worker, preserving strict per-(partition, groupId)
 * ordering.
 */
object GroupBucket {

  fun of(groupId: String?, workerCount: Int): Int {
    val effectiveWorkerCount = maxOf(1, workerCount)
    if (groupId == null) {
      return 0
    }
    return Math.floorMod(groupId.hashCode(), effectiveWorkerCount)
  }
}
