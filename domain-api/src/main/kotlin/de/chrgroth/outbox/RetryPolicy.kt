package de.chrgroth.outbox

import java.time.Duration

@Suppress("MagicNumber")
data class RetryPolicy(
    val maxAttempts: Int = 5,
    val backoff: List<Duration> = listOf(
        Duration.ofSeconds(5),
        Duration.ofSeconds(10),
        Duration.ofSeconds(30),
        Duration.ofSeconds(60),
    ),
)
