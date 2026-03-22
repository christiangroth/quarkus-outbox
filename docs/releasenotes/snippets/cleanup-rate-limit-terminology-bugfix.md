* Replaced `DispatchResult.RateLimited` with `DispatchResult.Paused(reason, pausedUntil)` – applications can now express any pause scenario using the generic ACTIVE / PAUSED model.
* Renamed metric `outbox_tasks_rate_limited_total` to `outbox_tasks_paused_total`.
