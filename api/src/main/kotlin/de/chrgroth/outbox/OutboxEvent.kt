package de.chrgroth.outbox

interface OutboxEvent {
    val key: String
    fun deduplicationKey(): String
}
