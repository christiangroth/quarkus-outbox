package de.chrgroth.quarkus.outbox.domain

data class OutboxError(
    val message: String,
    val cause: Throwable? = null,
)
