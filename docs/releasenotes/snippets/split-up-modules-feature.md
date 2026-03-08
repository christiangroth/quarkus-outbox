* split-up-modules: Renamed module `api` to `domain-api`.
* split-up-modules: Split module `impl` into `domain-impl` (core business logic) and `adapter-out-persistence-mongodb` (MongoDB persistence adapter).
* split-up-modules: Added `deleteArchiveEntriesOlderThan` to `OutboxRepository` interface.
* split-up-modules: MongoDB index initializer now syncs indexes on startup, dropping any unexpected indexes in addition to creating missing ones.
