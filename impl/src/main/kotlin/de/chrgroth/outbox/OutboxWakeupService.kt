package de.chrgroth.outbox

import jakarta.enterprise.context.ApplicationScoped
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentHashMap

@ApplicationScoped
class OutboxWakeupService {

    private val channels: MutableMap<String, Channel<Unit>> = ConcurrentHashMap()

    fun getOrCreate(partition: OutboxPartition): Channel<Unit> =
        channels.getOrPut(partition.key) { Channel(Channel.CONFLATED) }

    fun signal(partition: OutboxPartition) {
        getOrCreate(partition).trySend(Unit)
    }
}
