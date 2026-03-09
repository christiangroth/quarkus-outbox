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
