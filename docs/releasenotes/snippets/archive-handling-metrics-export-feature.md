* Archive handling can be enabled or disabled via the `outbox.archive.enabled` property (defaults to `true`).
* The archive cleanup cron job is skipped when archive handling is disabled.
* New metrics exported for archive handling: timer for cleanup job duration, counter for deleted archive tasks, counter for added archive tasks, and gauge for the current total number of archive tasks.
