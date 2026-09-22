* delayed-dispatch: Events can now be enqueued for delayed/scheduled dispatch via an optional `notBefore` instant, instead of always dispatching immediately.
* delayed-dispatch: Added `cancel` and `reschedule` operations to cancel or move a not-yet-dispatched task by its deduplication key, e.g. when a schedule it was derived from changes.
