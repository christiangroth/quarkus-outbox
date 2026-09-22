* group-id-ordering: Added an optional per-task `groupId` so unrelated tasks within the same outbox partition can be processed concurrently, while tasks sharing a `groupId` stay strictly ordered.
* group-id-ordering: Partitions can now be configured to run with more than one worker via `ApplicationOutboxPartition.workerCount` (defaults to `1`, unchanged behavior).
