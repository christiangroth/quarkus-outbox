package de.chrgroth.quarkus.outbox.domain

interface OutboxEvent {
    val key: String
    fun deduplicationKey(): String
}
