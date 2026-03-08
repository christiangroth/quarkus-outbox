# Quarkus Outbox

A Quarkus library that implements the [Transactional Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for reliable, at-least-once event dispatching backed by MongoDB.

## Features

- **Guaranteed delivery** – tasks are persisted before dispatch; no event is lost on crash
- **Deduplication** – duplicate events with the same key are silently dropped
- **Priority support** – `HIGH` and `NORMAL` priority tasks; high-priority tasks are dispatched first
- **Retry with configurable backoff** – failed dispatches are retried with per-attempt delays
- **Rate-limit handling** – partitions can be paused automatically and resumed after a back-off window
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
class MyTaskDispatcher : OutboxTaskDispatcher {

    override val partitions = MyPartition.entries

    override fun dispatch(task: OutboxTask): OutboxTaskResult {
        return try {
            httpClient.post(task.payload)
            OutboxTaskResult.Success
        } catch (e: RateLimitException) {
            OutboxTaskResult.RateLimited(Duration.ofSeconds(e.retryAfterSeconds))
        } catch (e: Exception) {
            OutboxTaskResult.Failed(e.message ?: "unknown error", e)
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
app.outbox.archive-retention-days=90
```

## Architecture

See [docs/arc42.md](docs/arc42.md) for the full architecture documentation including component diagrams and sequence diagrams.

## Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `app.outbox.archive-retention-days` | – (required) | Number of days to retain archived tasks |

### Partition Configuration

Implement `OutboxPartition` to configure per-partition behaviour:

| Property | Default | Description |
|----------|---------|-------------|
| `key` | – (required) | Unique partition identifier |
| `pauseOnRateLimit` | `true` | Pause the entire partition on a rate-limited response |
| `throttleInterval` | `null` | Minimum delay between consecutive dispatches |

### Retry Policy

Provide a CDI bean of type `RetryPolicy` to override the defaults:

| Setting | Default |
|---------|---------|
| `maxAttempts` | 5 |
| `backoff[0]` | 5 s |
| `backoff[1]` | 10 s |
| `backoff[2]` | 30 s |
| `backoff[3]` | 60 s |

## Metrics

| Metric | Type | Tags |
|--------|------|------|
| `outbox_tasks_enqueued_total` | Counter | `partition` |
| `outbox_tasks_processed_total` | Counter | `partition` |
| `outbox_tasks_failed_total` | Counter | `partition` |
| `outbox_tasks_rate_limited_total` | Counter | `partition` |
| `outbox_partition_status` | Gauge | `partition` (1 = active, 0 = paused) |

## Building

```bash
# Full build (includes tests and static analysis)
GHCR_PAT=<github-pat> ./gradlew build

# Tests only
GHCR_PAT=<github-pat> ./gradlew test
```

## License

[MIT](LICENSE)
