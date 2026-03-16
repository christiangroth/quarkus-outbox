package de.chrgroth.quarkus.outbox.domain

enum class OutboxTaskStatus {
    PENDING,
    PROCESSING,
    DONE,
    FAILED,
}
