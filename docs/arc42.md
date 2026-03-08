# arc42 – Quarkus Outbox

## 1. Introduction and Goals

**Quarkus Outbox** is a reusable library that implements the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Quarkus applications. It provides reliable, at-least-once event dispatching by persisting tasks in MongoDB before dispatching them to an external system.

**Key goals:**

| Goal | Description |
|------|-------------|
| Reliable dispatch | Tasks are persisted before dispatch – no event is lost on crash |
| Deduplication | Duplicate events with the same key are silently dropped |
| Rate-limit handling | Partitions can be paused and auto-resumed on rate-limited responses |
| Prioritisation | Tasks carry `NORMAL` or `HIGH` priority; high-priority tasks are dispatched first |
| Retry with backoff | Configurable retry policy with per-attempt backoff delays |
| Observability | Micrometer metrics and slow-query detection built in |
| Modularity | Clean hexagonal structure: domain-api → domain-impl ← adapter-out-persistence-mongodb |

---

## 2. Constraints

- Requires Quarkus and a MongoDB instance.
- Concurrency model is partition-based and coroutine-driven; one worker coroutine per partition.
- The library is designed for embedding (not a standalone service); the application must provide an `OutboxTaskDispatcher` CDI bean.

---

## 3. Context

The following diagram shows Quarkus Outbox within its operational context.

![System Context](https://kroki.io/c4plantuml/svg/eNpdkU1Ow0AMhfc5hdVVKlXiCi0_EiwQAbpHTmKC1WSm2J5QdtyBG3ISPP0JKrux5s333vMs1VAsDX1RGFtP8PypRgNcxWC0M_j5-obHhLJJCg_J6rgriopEYyibninYAmar7bbnBo1jmPl4z41EJRm5IUjKoQN7o-Pr2bw4OJRxP7v-HJ8JT9Qz1h6GRneAlnWL1rxl0sgIa8Gg2GQ_7I_PoEIzkjDxX252Vg4xdLGtc6h8ur7M9ByfXeJktSjYEbxGAUPdKGBoAcW9RjpH-Zo6ym19cB83Plxm4u16XcGqulvAQKoZWEvckCzAucoD9yhOK7zXtLWpPoX3RInKfde5y7LqdPtXQAhbvfgQNtJD1n_SKeBpXWVWZeCSQus__AtjJqxZ)

**Interfaces:**

| Interface | Direction | Description |
|-----------|-----------|-------------|
| `Outbox` (API) | inbound | Used by the application to enqueue events |
| `OutboxTaskDispatcher` (API) | inbound | Implemented by the application; called per task dispatch |
| `OutboxRepository` (SPI) | outbound | Persistence port; implemented by the MongoDB adapter |
| MongoDB | outbound | Stores tasks, archives, and partition state |

---

## 4. Solution Strategy

| Decision | Rationale |
|----------|-----------|
| Hexagonal Architecture | Keeps domain logic independent of MongoDB; the adapter can be swapped |
| Kotlin Coroutines | Non-blocking, lightweight partition workers without thread-per-partition overhead |
| CONFLATED Channel | Signals are coalesced: many rapid enqueues produce only one dispatch cycle |
| Atomic claim via MongoDB | `findOneAndUpdate(status=PENDING → PROCESSING)` prevents duplicate processing |
| Partition-level pause | Rate limiting only affects the affected partition, not the whole system |

---

## 5. Building Blocks

### 5.1 Module Overview

![Component Diagram](https://kroki.io/c4plantuml/svg/eNp1VMFu2zAMvecriJxaIMbuO7VxdsiALlk6YMdBkZlMiCxpopQ1GAbsH_aH-5JRSuzITnszqadH8fHRDxSED7HVk0lQQSPUtnXWoAmwUGLvRQv__vyFz1H4QyRYxbC1L_Bkm6iRJpPamiCUQf9tbqNphD_dCadmMG1sy_mKg-k9_JrAlffOZo7lTkhk4Jlxyl_KBPQpm4K1Vy2zweN6-R7Q_IgYGe28lUj0CV_CDEjtjdDT-wG5R2eH1BvOkArWn26LoCdFAY1EcNaHEVejyIkgv6MfMn4RdFj0Zzes3VFH-ftVmVTr9FWnFL0l1Bm46oNUo14sYY7CpO-V52dQ8CIgdQpZP4MWg1eSZmC3hP7IvY76K7AX-nWXyepofhlILYhySQ7bqtOk8khRB9Bqh_IkNY64f1p_wIKYTaaCsuZrzo9bqK3nZlmgaisIG3AdnPnTfPjGeMzSckenvsBztrHbXPLjAhskDARsdrZ44PkRMPnZ-27Ezc0IE11P_cj6qiPW5_RHux2TP7P-aR8aEGcoTyEaZfZvDr-1Zm_5pmiEY-dU3Hzlrmas8nmzvXFEzidD892n9P2axcunZdBiDslf2DKFyKraHdzczG_doB7YbrirPUty0hhauikSdpAiXe5mgeisMrC7FFpTue8X8HXwA7jP830nZFDHtAcXdD_KsnSDGgNepvrB8JL0-ELf8sag7wc0Df8v_wPcwNJs)

### 5.2 Key Components

| Component | Module | Responsibility |
|-----------|--------|----------------|
| `Outbox` | domain-api | Primary inbound port: enqueue, processNext, signal |
| `OutboxRepository` | domain-api | Outbound persistence port |
| `OutboxTaskDispatcher` | domain-api | Outbound dispatch port (implemented by the application) |
| `OutboxImpl` | domain-impl | CDI bean; orchestrates processor, metrics, and partition observers |
| `OutboxProcessor` | domain-impl | Stateless claim-dispatch-result lifecycle |
| `OutboxPartitionWorker` | domain-impl | One coroutine worker per partition; listens to CONFLATED channel |
| `OutboxWakeupService` | domain-impl | Manages per-partition `Channel<Unit>` for wakeup signals |
| `OutboxStartupRecovery` | domain-impl | Resets stale `PROCESSING` tasks and restores partition states at startup |
| `OutboxArchiveCleanupJob` | domain-impl | Scheduled daily job that prunes old archive entries |
| `MongoOutboxRepository` | adapter-out-persistence-mongodb | MongoDB implementation of `OutboxRepository` |
| `OutboxIndexInitializer` | adapter-out-persistence-mongodb | Creates/syncs required MongoDB indexes on startup |
| `OutboxQueryMetrics` | adapter-out-persistence-mongodb | Wraps repository calls with Micrometer timers and slow-query counters |

---

## 6. Runtime View

### 6.1 Enqueue and Dispatch Flow

![Enqueue and Dispatch Sequence](https://kroki.io/plantuml/svg/eNqVVMtuwjAQvPsr9lQlEvQDemip2iJVqtoKKnFewgIWju3aGx5_3w3hkUCo4JBIWe_MzoxXUb3IGLjIjWLNhuDN_hZUENzBq44eOZtD37iVUl7adKY9WoYXo8lyo_RV8Nit33NvWsoD8i5qdmHTcjjCBRV-SGGpM2o5_y4rrJ0dubCg0NLxg3GxVysNqpIH3ceaKngAqqwlfk_YAVpKYwc8bozDSapq_Qf0UfwtHDVU91QIBwk40TZSYLowtZGKYKKeWTTHuU2UwHamK3al2liOQk4yFVQ2R2vJ3AfKSC8pSZVxzsNqrmUpWAKOgEvUBseGFFygOTXqg8soxk9acz2xye6u0gPRP4lnBnVe9w1wRcIi-AJ5c1mkd68mKUElPRqGYZGVyuUL2nFnI88R_5lyckZMh5lkIsEAmT50rmUpbhhcQyWBOGyep0whvU5IoCisk8JUUjpQMfCVcI9FpMMWNG9pa6kvK3OTmwqQ5BIlzuhKGVMB7fTvgB2wsnSDyszTVo6dqPLpyav83f0BLY3ECA==)

### 6.2 Startup Recovery

On every application start, `OutboxStartupRecovery` (with `@Priority(1)`) runs **before** `OutboxPartitionWorker`:

1. Calls `outbox.resetStaleProcessingTasks()` – resets any tasks left in `PROCESSING` status from a previous crash back to `PENDING`.
2. For each partition registered in `OutboxTaskDispatcher.partitions`:
   - If the partition has no persisted state **or** is `ACTIVE` → `activatePartition` + `signal`.
   - If the partition is `PAUSED` and `pausedUntil` is in the past / null → `activatePartition` + `signal`.
   - If the partition is `PAUSED` and `pausedUntil` is in the future → schedule a delayed coroutine to activate later.

### 6.3 Rate Limiting

When an `OutboxTaskDispatcher.dispatch` call returns `OutboxTaskResult.RateLimited(retryAfter)`:

- The task is rescheduled with `nextRetryAt = now + retryAfter`.
- **If `partition.pauseOnRateLimit == true`**: the partition is persisted as `PAUSED`, the gauge is set to `0`, and a coroutine schedules `activatePartition + signal` after `retryAfter`.
- **If `partition.pauseOnRateLimit == false`**: only the individual task is rescheduled; other tasks in the same partition continue to be processed.

---

## 7. Deployment View

Quarkus Outbox is a set of JARs added as Gradle/Maven dependencies. The consuming application:

1. Adds `domain-api`, `domain-impl`, and `adapter-out-persistence-mongodb` as dependencies.
2. Provides a CDI bean implementing `OutboxTaskDispatcher`.
3. Ensures a MongoDB instance is reachable (configured via Quarkus standard properties).
4. Sets `app.outbox.archive-retention-days` in `application.properties`.

MongoDB collections created automatically:

| Collection | Purpose |
|------------|---------|
| `outbox` | Active tasks (PENDING / PROCESSING) |
| `outbox_archive` | Completed and permanently-failed tasks |
| `outbox_partitions` | Per-partition pause state |

---

## 8. Cross-Cutting Concepts

### Retry Policy

Configured via `RetryPolicy` (injectable or default). Default:

| Attempt | Delay before retry |
|---------|--------------------|
| 1 | 5 s |
| 2 | 10 s |
| 3 | 30 s |
| 4+ | 60 s |
| ≥ maxAttempts (5) | task archived as FAILED |

### Metrics (Micrometer)

| Metric | Type | Tags |
|--------|------|------|
| `outbox_tasks_enqueued_total` | Counter | `partition` |
| `outbox_tasks_processed_total` | Counter | `partition` |
| `outbox_tasks_failed_total` | Counter | `partition` |
| `outbox_tasks_rate_limited_total` | Counter | `partition` |
| `outbox_partition_status` | Gauge | `partition` (1=active, 0=paused) |
| `mongodb.query` | Timer | `operation` |
| `mongodb.slow.queries` | Counter | `operation` |

### Deduplication

Before inserting a task, `MongoOutboxRepository.enqueue` checks for an existing `PENDING` or `PROCESSING` task with the same `(partition, deduplicationKey)`. If one exists, the insert is skipped and `false` is returned.

---

## 9. Architecture Decisions

| ID | Decision | Rationale |
|----|----------|-----------|
| ADR-1 | MongoDB as persistence store | Native fit with Quarkus Panache; flexible document model |
| ADR-2 | Partition-based workers | Isolation between event streams; independent rate-limit handling |
| ADR-3 | CONFLATED channel for wakeup | Prevents channel overflow under high enqueue rates |
| ADR-4 | Atomic claim in MongoDB | Single `findOneAndUpdate` call ensures no two workers process the same task |
| ADR-5 | Sealed `OutboxTaskResult` | Exhaustive dispatch result handling enforced by the compiler |

---

## 10. Quality Requirements

| Requirement | Mechanism |
|-------------|-----------|
| Reliability | At-least-once delivery, startup recovery of stale tasks |
| No duplicate processing | Atomic MongoDB claim, deduplication key |
| Observability | Micrometer counters/gauges, slow-query detection |
| Code quality | Detekt static analysis, Kover ≥ 40% coverage gate |
| Maintainability | Hexagonal architecture, Kotlin coroutines, modular Gradle build |
