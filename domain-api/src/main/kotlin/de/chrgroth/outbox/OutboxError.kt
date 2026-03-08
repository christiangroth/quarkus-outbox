package de.chrgroth.outbox

data class OutboxError(
    val message: String,
    val cause: Throwable? = null,
)
