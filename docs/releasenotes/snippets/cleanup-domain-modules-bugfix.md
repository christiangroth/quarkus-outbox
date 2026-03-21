* cleanup-domain-modules: Changed base package to `de.chrgroth.quarkus.outbox.domain`.
* cleanup-domain-modules: Applied hexagonal architecture with inbound port (`Outbox`) in `port.in` and outbound ports (`OutboxRepository`, `OutboxTaskDispatcher`, `OutboxPartitionObserver`) in `port.out`.
* cleanup-domain-modules: The `Outbox` inbound port now only exposes user-facing methods: `enqueue`, `findPartition`, `activatePartition`, and `archiveFailedTasks`.
