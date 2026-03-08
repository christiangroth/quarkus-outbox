package de.chrgroth.outbox

enum class OutboxTaskStatus {
    PENDING,
    PROCESSING,
    DONE,
    FAILED,
}
