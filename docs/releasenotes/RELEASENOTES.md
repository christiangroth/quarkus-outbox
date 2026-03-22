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
