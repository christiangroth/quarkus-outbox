# arc42 – Quarkus Outbox

## 1. Introduction and Goals

**Quarkus Outbox** is a reusable library that implements the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Quarkus applications. It provides reliable, at-least-once event dispatching by persisting tasks in MongoDB before dispatching them to an external system.

**Key goals:**

| Goal | Description |
|------|-------------|
| Reliable dispatch | Tasks are persisted before dispatch – no event is lost on crash |
| Deduplication | Duplicate events with the same key are silently dropped |
| Partition pause / resume | Partitions can be paused (with optional reason and resume time) and auto-resumed |
| Prioritisation | Tasks carry `HIGH`, `MEDIUM` (default), or `LOW` priority; high-priority tasks are dispatched first |
| Retry with backoff | Configurable retry policy with per-attempt backoff delays |
| Observability | Micrometer metrics and slow-query detection built in |
| Modularity | Clean hexagonal structure across five dedicated modules |

---

## 2. Constraints

- Requires Quarkus and a MongoDB instance.
- Concurrency model is partition-based and coroutine-driven; one worker coroutine per partition.
- The library is designed for embedding (not a standalone service); the application must provide an `ApplicationOutboxDispatcher` CDI bean.

---

## 3. Context

The following diagram shows Quarkus Outbox within its operational context.

**Interfaces:**

| Interface | Direction | Description |
|-----------|-----------|-------------|
| `ApplicationOutboxClient` | inbound | Used by the application to enqueue events and query partition state |
| `ApplicationOutboxDispatcher` | outbound | Implemented by the application; provides partitions and dispatches tasks |
| MongoDB | outbound | Stores tasks, archives, and partition state |

---

## 4. Solution Strategy

| Decision | Rationale |
|----------|-----------|
| Hexagonal Architecture | Keeps domain logic independent of MongoDB; adapters can be swapped |
| Kotlin Coroutines | Non-blocking, lightweight partition workers without thread-per-partition overhead |
| CONFLATED Channel | Signals are coalesced: many rapid enqueues produce only one dispatch cycle |
| Atomic claim via MongoDB | `findOneAndUpdate(status=PENDING → PROCESSING)` prevents duplicate processing |
| Partition-level pause | Pausing only affects the concerned partition, not the whole system |
| CDI async events | Fire-and-forget domain events for client applications to react to outbox lifecycle changes |

---

## 5. Building Blocks

### 5.1 Module Overview

| Module | Role |
|--------|------|
| `domain-api` | Public API: inbound/outbound ports, domain types, CDI event classes |
| `domain-impl` | Domain logic: adapters for execution, archiving, and startup recovery |
| `adapter-out-executor` | Coroutine infrastructure: `CoroutinesAdapter` implementing `CoroutinesPort` |
| `adapter-in-scheduler` | Scheduler adapter: `ArchiverJob` for periodic archive cleanup |
| `adapter-out-persistence-mongodb` | MongoDB persistence: repository adapters for tasks, archives, and partitions |

### 5.2 Key Components

| Component | Module | Responsibility |
|-----------|--------|----------------|
| `ApplicationOutboxClient` | domain-api | Inbound port: enqueue events, query partition infos |
| `ApplicationOutboxDispatcher` | domain-api | Outbound port: provide partitions, dispatch tasks |
| `ApplicationOutboxClientAdapter` | domain-impl | Implements `ApplicationOutboxClient`; delegates to `OutboxControllerAdapter` |
| `OutboxControllerAdapter` | domain-impl | Orchestrates enqueue, dispatch, partition activation, metrics, and CDI events |
| `ArchiverAdapter` | domain-impl | Implements `ArchiverPort`; delegates archive cleanup to persistence port |
| `PartitionWorkerStarter` | domain-impl | Startup recovery + one coroutine worker per partition |
| `CoroutinesAdapter` | adapter-out-executor | Manages the coroutine scope and per-partition `CONFLATED` channels |
| `ArchiverJob` | adapter-in-scheduler | Scheduled daily job that prunes old archive entries via `ArchiverPort` |
| `TaskRepositoryAdapter` | adapter-out-persistence-mongodb | MongoDB operations for outbox tasks |
| `ArchivedTaskRepositoryAdapter` | adapter-out-persistence-mongodb | MongoDB operations for archived tasks |
| `PartitionRepository` | adapter-out-persistence-mongodb | MongoDB operations for partition state |
| `IndexInitializationStarter` | adapter-out-persistence-mongodb | Creates/syncs required MongoDB indexes on startup |
| `MetricsRecorder` | adapter-out-persistence-mongodb | Wraps repository calls with Micrometer timers and slow-query counters |

### 5.3 Ports

**Inbound ports** (in `domain-api` / `domain-impl`, package `port.in`):

| Port | Description |
|------|-------------|
| `ApplicationOutboxClient` | Application-facing API for enqueueing events and querying partitions |
| `ArchiverPort` | Trigger archive cleanup (used by `ArchiverJob`) |

**Outbound ports** (in `domain-impl`, package `port.out`):

| Port | Description |
|------|-------------|
| `ApplicationOutboxDispatcher` | Implemented by the application; provides partitions and dispatches tasks |
| `CoroutinesPort` | Coroutine scope, per-partition signals |
| `TaskRepositoryPort` | CRUD and lifecycle operations on outbox tasks |
| `ArchivedTaskRepositoryPort` | Append and cleanup of archived tasks |
| `PartitionRepositoryPort` | Find, create, pause, and resume partition records |

---

## 6. CDI Events

The following fire-and-forget events are published asynchronously for client applications to observe:

| Event | Fired when |
|-------|------------|
| `OutboxPartitionActivatedEvent` | A partition transitions to active |
| `OutboxPartitionPausedEvent` | A partition is paused (includes `reason` and `pausedUntil`) |
| `OutboxTaskEnqueuedEvent` | A task is successfully enqueued (not fired on deduplication discard) |
| `OutboxTaskDispatchedEvent` | A task is dispatched successfully and archived |
| `OutboxTaskRetryScheduledEvent` | A task fails but will be retried |
| `OutboxTaskFailedEvent` | A task permanently fails after exhausting all retries |

All events carry `partition: ApplicationOutboxPartition` and `eventType: String`.

---

## 7. Runtime View

### 7.1 Enqueue and Dispatch Flow

1. Application calls `ApplicationOutboxClient.enqueue(event)`.
2. `ApplicationOutboxClientAdapter` delegates to `OutboxControllerAdapter.enqueue()`.
3. `OutboxControllerAdapter` calls `TaskRepositoryPort.enqueue()`. If the task is a duplicate it is silently discarded; otherwise the `CoroutinesPort` is signalled and `OutboxTaskEnqueuedEvent` is fired.
4. The `PartitionWorkerStarter` coroutine loop wakes up and calls `OutboxControllerAdapter.dispatchTask()` repeatedly until no task remains.
5. `OutboxControllerAdapter` claims a task via `TaskRepositoryPort.claim()`, calls `ApplicationOutboxDispatcher.dispatch()`, and handles the result:
   - **Success** → archives via `ArchivedTaskRepositoryPort.append()`, deletes the task, fires `OutboxTaskDispatchedEvent`.
   - **Pause** → reschedules the task, optionally pauses the partition, fires `OutboxPartitionPausedEvent`, schedules delayed reactivation when `pausedUntil` is set.
   - **Failed** → retries with backoff (fires `OutboxTaskRetryScheduledEvent`) or archives as permanently failed (fires `OutboxTaskFailedEvent`).

### 7.2 Startup Recovery

On every application start, `PartitionWorkerStarter.onStart()` (with `@Priority(1)`):

1. Calls `OutboxControllerAdapter.resetStaleProcessingTasks()` – resets any tasks left in `PROCESSING` status from a previous crash back to `PENDING`.
2. For each partition returned by `ApplicationOutboxDispatcher.getAllPartitions()`:
   - If `ACTIVE` → `activatePartition` + `signal`.
   - If `PAUSED` and `pausedUntil` is `null` → leaves partition paused (manual pause).
   - If `PAUSED` and `pausedUntil` has passed → immediately reactivates.
   - If `PAUSED` and `pausedUntil` is in the future → schedules a delayed coroutine to reactivate later.
3. Starts one coroutine worker per partition.

### 7.3 Archive Cleanup

`ArchiverJob` runs daily at 01:00 UTC. It calls `ArchiverPort.deleteOlderThan(cutoff)` where `cutoff = now - outbox.archive.retention-days`. The `ArchiverAdapter` delegates to `ArchivedTaskRepositoryPort.deleteOlderThan()`.

### 7.4 Partition Pause

When `ApplicationOutboxDispatcher.dispatch()` returns `DispatchResult.Paused(reason, pausedUntil)`:

- The partition is persisted as `PAUSED` (with optional `reason` and `pausedUntil`), the gauge is set to `0`, and the task is rescheduled with `nextRetryAt = pausedUntil` (or immediately if `pausedUntil` is `null`).
- When `pausedUntil` is set, a coroutine schedules `activatePartition + signal` at that time.

The `reason` and `pausedUntil` fields are application-controlled: they allow the application to encode any pause scenario (e.g. a rate-limit response from the target system) using the library's generic ACTIVE / PAUSED model.

---

## 8. Deployment View

Quarkus Outbox is a set of JARs added as Gradle/Maven dependencies. The consuming application:

1. Adds `domain-api`, `domain-impl`, `adapter-out-executor`, `adapter-out-persistence-mongodb`, and optionally `adapter-in-scheduler` as dependencies.
2. Provides a CDI bean implementing `ApplicationOutboxDispatcher`.
3. Ensures a MongoDB instance is reachable (configured via Quarkus standard properties).
4. Sets `outbox.archive.retention-days` in `application.properties` when using `adapter-in-scheduler`.

MongoDB collections created automatically:

| Collection | Purpose |
|------------|---------|
| `outbox` | Active tasks (PENDING / PROCESSING) |
| `outbox_archive` | Completed and permanently-failed tasks |
| `outbox_partitions` | Per-partition pause state |

---

## 9. Cross-Cutting Concepts

### Retry Policy

Configured via `RetryPolicy` (default in `domain-impl`). Default:

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
| `outbox_tasks_paused_total` | Counter | `partition` |
| `outbox_partition_status` | Gauge | `partition` (1=active, 0=paused) |
| `mongodb.query` | Timer | `operation` |
| `mongodb.slow.queries` | Counter | `operation` |

### Deduplication

Before inserting a task, `TaskRepositoryAdapter.enqueue` checks for an existing `PENDING` or `PROCESSING` task with the same `(partition, deduplicationKey)`. If one exists, the insert is skipped and `false` is returned.

---

## 10. Architecture Decisions

| ID | Decision | Rationale |
|----|----------|-----------|
| ADR-1 | MongoDB as persistence store | Native fit with Quarkus Panache; flexible document model |
| ADR-2 | Partition-based workers | Isolation between event streams; independent pause handling |
| ADR-3 | CONFLATED channel for wakeup | Prevents channel overflow under high enqueue rates |
| ADR-4 | Atomic claim in MongoDB | Single `findOneAndUpdate` call ensures no two workers process the same task |
| ADR-5 | Sealed `DispatchResult` | Exhaustive dispatch result handling enforced by the compiler |
| ADR-6 | CDI async events | Decoupled lifecycle notifications; applications observe only the events they care about |
| ADR-7 | Hexagonal modules | Each module has a single clear responsibility; adapters are interchangeable |

---

## 11. Quality Requirements

| Requirement | Mechanism |
|-------------|-----------|
| Reliability | At-least-once delivery, startup recovery of stale tasks |
| No duplicate processing | Atomic MongoDB claim, deduplication key |
| Observability | Micrometer counters/gauges, slow-query detection, CDI lifecycle events |
| Code quality | Detekt static analysis, Kover coverage gate |
| Maintainability | Hexagonal architecture, Kotlin coroutines, modular Gradle build |
