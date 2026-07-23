# 0.8.4 (2026.07.23)

## Bugfixes / Chore
* issue-45-20260723-0628: Fixed a bug where a failed task's scheduled retry never woke up its partition worker, so a task with a fixed/singleton deduplication key could get permanently stuck after a single transient failure until the application was restarted or an unrelated task happened to be enqueued on the same partition.
* issue-45-20260723-0628: The partition worker is now also re-armed for any still-pending retries when the application starts up, closing the same gap across restarts.



---
# 0.8.3 (2026.07.23)

## Bugfixes / Chore
* issue-44-20260723-0453: Fixed a bug where an unexpected error while dispatching an outbox task (e.g. a database connection issue) could silently stop all outbox processing for every partition until the application was restarted.
* issue-44-20260723-0453: An unexpected error while dispatching an outbox task is now treated as a regular failure, so the task's retry count is incremented and it is retried or archived as failed instead of getting stuck.



---
# 0.8.2 (2026.07.23)

## Bugfixes / Chore
* issue-41-20260723-0428: Fixed `outbox.archive.enabled=false` not being respected: completed/failed tasks are no longer written to the archive when the property is disabled, not just excluded from the retention cleanup job.
* issue-41-20260723-0428: The archive collection is now cleared once at startup if archiving has been disabled and leftover entries remain.
* issue-41-20260723-0428: Added a link to the release notes in the README features section.



---
# 0.8.1 (2026.07.08)

## Bugfixes / Chore
* persistent-partition-event-counts: Fixed frequent "Slow MongoDB query" warnings for `outbox.task.countByEventType` by maintaining per-partition event type counts incrementally instead of recomputing them on every read.
* persistent-partition-event-counts: Added missing MongoDB indexes covering event-type aggregation and partition task listing queries.
* persistent-partition-event-counts: Added a daily reconciliation job (`outbox.reconciliation.enabled`, default `true`) that corrects any drift between persisted and actual event type counts.



---
# 0.8.0 (2026.03.26)

## New Features
* `partitionInfos()` on `ApplicationOutboxClient` now returns event counts per partition, including a total `eventCount` and a breakdown by event type key in `eventPerTypeCount`.



---
# 0.7.3 (2026.03.25)

## Bugfixes / Chore
* Fixed ClassFormat errors when using the MongoDB persistence adapter with Kotlin by separating Panache repository classes from port adapter classes.



---
# 0.7.2 (2026.03.24)

## Bugfixes / Chore
* Fixed build compatibility issue when using the MongoDB persistence adapter with Java 25.



---
# 0.7.1 (2026.03.22)

## Bugfixes / Chore
* All metric names now use dot notation consistently (e.g. `outbox.tasks.enqueued`).
* Task counters now include both `partition` and `priority` tags on a single metric instead of separate per-partition and per-partition-per-priority counters.
* Metrics documentation in arc42 updated to match actual metric names.



---
# 0.7.0 (2026.03.22)

## New Features
* Added `OutboxTaskRescheduledEvent`, fired when a task is rescheduled because the partition was paused.
* Fixed documentation: clarified that only task events carry `eventType`, while partition events do not.



---
# 0.6.1 (2026.03.22)

## Bugfixes / Chore
* Replaced `DispatchResult.RateLimited` with `DispatchResult.Paused(reason, pausedUntil)` – applications can now express any pause scenario using the generic ACTIVE / PAUSED model.
* Renamed metric `outbox_tasks_rate_limited_total` to `outbox_tasks_paused_total`.



---
# 0.6.0 (2026.03.22)

## New Features
* Priority levels are now `HIGH`, `MEDIUM` (default), and `LOW`; `MEDIUM` replaces the former `NORMAL` value.
* Task-count metrics now carry a `priority` tag for per-priority breakdown; aggregate (all-priorities) variants are available as `outbox_tasks_*_all_total`.



---
# 0.5.0 (2026.03.22)

## New Features
* Archive handling can be enabled or disabled via the `outbox.archive.enabled` property (defaults to `true`).
* The archive cleanup cron job is skipped when archive handling is disabled.
* New metrics exported for archive handling: timer for cleanup job duration, counter for deleted archive tasks, counter for added archive tasks, and gauge for the current total number of archive tasks.

## Bugfixes / Chore
* All configuration properties now use a consistent `outbox.*` naming prefix.
* Property defaults are exclusively defined in each module's `application.properties`.
* Documentation updated with an accurate configuration reference table for client projects.



---
# 0.4.0 (2026.03.21)

## New Features
* The dispatcher now receives an `ApplicationOutboxEvent` instead of the internal `OutboxTask`, keeping task tracking details internal to the framework.
* Serialization of the event payload is defined via `ApplicationOutboxEvent.serializePayload`; deserialization is handled by the new `ApplicationOutboxDispatcher.deserialize` method.



---
# 0.3.1 (2026.03.21)

## Bugfixes / Chore
* cleanup-domain-modules: Changed base package to `de.chrgroth.quarkus.outbox.domain`.
* cleanup-domain-modules: Applied hexagonal architecture with inbound port (`Outbox`) in `port.in` and outbound ports (`OutboxRepository`, `OutboxTaskDispatcher`, `OutboxPartitionObserver`) in `port.out`.
* cleanup-domain-modules: The `Outbox` inbound port now only exposes user-facing methods: `enqueue`, `findPartition`, `activatePartition`, and `archiveFailedTasks`.



---
# 0.3.0 (2026.03.09)

## New Features
* add-sources-publishing: Sources JARs are now published alongside the main artifacts.



---
# 0.2.1 (2026.03.09)

## Bugfixes / Chore
* remove-kotlin-bom: Removed explicit Kotlin BOM from convention plugin, as it is already managed by the Quarkus BOM.



---
# 0.2.0 (2026.03.08)

## New Features
* review-and-cleanup: Added unit tests for `OutboxImpl` and `OutboxStartupRecovery`.
* review-and-cleanup: Added `docs/arc42.md` architecture documentation with PlantUML diagrams.
* review-and-cleanup: Added `README.md` with getting-started guide and configuration reference.



---
# 0.1.0 (2026.03.08)

## New Features
* split-up-modules: Renamed module `api` to `domain-api`.
* split-up-modules: Split module `impl` into `domain-impl` (core business logic) and `adapter-out-persistence-mongodb` (MongoDB persistence adapter).
* split-up-modules: Added `deleteArchiveEntriesOlderThan` to `OutboxRepository` interface.
* split-up-modules: MongoDB index initializer now syncs indexes on startup, dropping any unexpected indexes in addition to creating missing ones.



---
# 0.0.2 (2026.03.08)

## Bugfixes / Chore
* fix-initial-build: Resolved build failures preventing initial project setup.



---
