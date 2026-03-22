* All metric names now use dot notation consistently (e.g. `outbox.tasks.enqueued`).
* Task counters now include both `partition` and `priority` tags on a single metric instead of separate per-partition and per-partition-per-priority counters.
* Metrics documentation in arc42 updated to match actual metric names.
