package de.chrgroth.quarkus.outbox.domain

interface ApplicationOutboxEvent {
    val key: String
    fun deduplicationKey(): String
}
