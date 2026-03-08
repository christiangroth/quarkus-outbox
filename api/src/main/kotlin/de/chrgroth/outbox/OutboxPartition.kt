package de.chrgroth.outbox

import java.time.Duration

interface OutboxPartition {
    val key: String

    /**
     * Controls whether the partition is paused when a rate-limited response is received.
     * Set to `false` for partitions where processing must continue even under rate limiting
     * (e.g. to avoid missing time-sensitive data). Defaults to `true`.
     */
    val pauseOnRateLimit: Boolean get() = true

    /**
     * Minimum delay between consecutive task dispatches for this partition.
     * Set to a positive [Duration] to proactively throttle outgoing requests and avoid rate limiting.
     * Defaults to `null` (no throttling).
     */
    val throttleInterval: Duration? get() = null
}
