package de.chrgroth.quarkus.outbox.adapter.out.mongodb

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class GroupBucketTests {

  @Test
  fun `tasks without a groupId always map to bucket 0`() {
    assertThat(GroupBucket.of(null, 1)).isEqualTo(0)
    assertThat(GroupBucket.of(null, 4)).isEqualTo(0)
    assertThat(GroupBucket.of(null, 16)).isEqualTo(0)
  }

  @Test
  fun `the same groupId always maps to the same bucket for a given worker count`() {
    val first = GroupBucket.of("user-42", 4)
    val second = GroupBucket.of("user-42", 4)

    assertThat(first).isEqualTo(second)
  }

  @Test
  fun `bucket is always within worker count bounds`() {
    val groupIds = listOf("a", "b", "c", "user-1", "user-2", "user-3", "", "!@#$%^&*()")

    groupIds.forEach { groupId ->
      val bucket = GroupBucket.of(groupId, 5)
      assertThat(bucket).isBetween(0, 4)
    }
  }

  @Test
  fun `worker count less than 1 is treated as 1, so every groupId maps to bucket 0`() {
    assertThat(GroupBucket.of("user-1", 0)).isEqualTo(0)
    assertThat(GroupBucket.of("user-1", -3)).isEqualTo(0)
  }

  @Test
  fun `different groupIds can map to different buckets`() {
    val buckets = (0 until 50).map { GroupBucket.of("user-$it", 4) }.toSet()

    assertThat(buckets).hasSizeGreaterThan(1)
  }
}
