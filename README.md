# Quarkus Outbox

A Quarkus library that implements the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for reliable, at-least-once event dispatching backed by MongoDB.

## Features

- **Guaranteed delivery** – tasks are persisted before dispatch; no event is lost on crash
- **Deduplication** – duplicate events with the same key are silently dropped
- **Priority support** – `HIGH`, `MEDIUM` (default), and `LOW` priority tasks; high-priority tasks are dispatched first
- **Retry with configurable backoff** – failed dispatches are retried with per-attempt delays
- **Partition pause / resume** – partitions can be paused (with optional reason and resume time) and auto-resumed
- **Per-partition throttling** – optional minimum delay between consecutive dispatches
- **Startup recovery** – stale `PROCESSING` tasks are reset and paused partitions are restored on restart
- **Scheduled archive cleanup** – completed and failed tasks are pruned after a configurable retention period
- **Observability** – built-in Micrometer counters/gauges and slow MongoDB query detection

## Modules

| Module | Artifact | Description |
|--------|----------|-------------|
| `domain-api` | `de.chrgroth.quarkus.outbox:domain-api` | Core interfaces and domain models |
| `domain-impl` | `de.chrgroth.quarkus.outbox:domain-impl` | CDI-managed orchestration and workers |
| `adapter-out-persistence-mongodb` | `de.chrgroth.quarkus.outbox:adapter-out-persistence-mongodb` | MongoDB persistence adapter |

## Getting Started

### 1. Add Dependencies

```kotlin
// build.gradle.kts
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/christiangroth/quarkus-outbox")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("de.chrgroth.quarkus.outbox:domain-api:<version>")
    implementation("de.chrgroth.quarkus.outbox:domain-impl:<version>")
    implementation("de.chrgroth.quarkus.outbox:adapter-out-persistence-mongodb:<version>")
}
```

### 2. Define Your Partitions

```kotlin
enum class MyPartition(override val key: String) : OutboxPartition {
    ORDERS("orders"),
    NOTIFICATIONS("notifications"),
}
```

### 3. Implement a Dispatcher

```kotlin
@ApplicationScoped
class MyTaskDispatcher : ApplicationOutboxDispatcher {

    override fun getAllPartitions() = MyPartition.entries

    override fun deserialize(partition: ApplicationOutboxPartition, eventType: String, payload: String): ApplicationOutboxEvent {
        // reconstruct your event from partition, eventType and payload
        return MyEvent(partition as MyPartition, payload)
    }

    override fun dispatch(event: ApplicationOutboxEvent): DispatchResult {
        return try {
            httpClient.post((event as MyEvent).payload)
            DispatchResult.Success
        } catch (e: ThrottledException) {
            DispatchResult.Paused(reason = "throttled", pausedUntil = Instant.now().plusSeconds(e.retryAfterSeconds))
        } catch (e: Exception) {
            DispatchResult.Failed(e.message ?: "unknown error", e)
        }
    }
}
```

### 4. Enqueue Events

```kotlin
@ApplicationScoped
class OrderService(private val outbox: Outbox) {

    fun placeOrder(order: Order) {
        // ... persist order ...
        outbox.enqueue(MyPartition.ORDERS, OrderCreatedEvent(order), order.toJson())
    }
}
```

### 5. Configure

```properties
# application.properties
outbox.archive.retention-days=90
```

## Configuration Reference

### Application Properties

| Property | Default | Description |
|----------|---------|-------------|
| `outbox.archive.retention-days` | `365` | Number of days to retain archived tasks (only when using `adapter-in-scheduler`) |
| `outbox.mongodb.slow-query-threshold-ms` | `100` | Threshold in milliseconds above which a MongoDB query is logged as slow |

### Partition Configuration

Implement `OutboxPartition` to configure per-partition behaviour:

| Property | Default | Description |
|----------|---------|-------------|
| `key` | – (required) | Unique partition identifier |
| `throttleInterval` | `null` | Minimum delay between consecutive dispatches |

### Retry Policy

Provide a CDI bean of type `RetryPolicy` to override the defaults:

| Setting | Default |
|---------|---------|
| `maxAttempts` | `5` |
| `backoff` | 5 s, 10 s, 30 s, 60 s |

## Building

```bash
# Full build (includes tests and static analysis)
GHCR_PAT=<github-pat> ./gradlew build

# Tests only
GHCR_PAT=<github-pat> ./gradlew test
```

## License

[MIT](LICENSE)
