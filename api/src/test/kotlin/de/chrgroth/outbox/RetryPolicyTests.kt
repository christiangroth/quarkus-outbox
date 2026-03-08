package de.chrgroth.outbox

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

class RetryPolicyTests {

    @Test
    fun `default configuration has maxAttempts 5 and correct backoff`() {
        val policy = RetryPolicy()

        assertThat(policy.maxAttempts).isEqualTo(5)
        assertThat(policy.backoff).containsExactly(
            Duration.ofSeconds(5),
            Duration.ofSeconds(10),
            Duration.ofSeconds(30),
            Duration.ofSeconds(60),
        )
    }

    @Test
    fun `custom configuration is stored correctly`() {
        val customBackoff = listOf(Duration.ofSeconds(1), Duration.ofSeconds(2))
        val policy = RetryPolicy(maxAttempts = 3, backoff = customBackoff)

        assertThat(policy.maxAttempts).isEqualTo(3)
        assertThat(policy.backoff).isEqualTo(customBackoff)
    }
}
