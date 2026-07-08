* persistent-partition-event-counts: Fixed frequent "Slow MongoDB query" warnings for `outbox.task.countByEventType` by maintaining per-partition event type counts incrementally instead of recomputing them on every read.
* persistent-partition-event-counts: Added missing MongoDB indexes covering event-type aggregation and partition task listing queries.
* persistent-partition-event-counts: Added a daily reconciliation job (`outbox.reconciliation.enabled`, default `true`) that corrects any drift between persisted and actual event type counts.
