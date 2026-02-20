// Copyright 2024-2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.pcg;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PartitionUtils.
 * These tests verify the partition distribution algorithm matches the Go implementation.
 */
class PartitionUtilsTest {

    @Test
    void testComposeKey() {
        assertEquals("mystream.mycg", PartitionUtils.composeKey("mystream", "mycg"));
        assertEquals("stream1.cg2", PartitionUtils.composeKey("stream1", "cg2"));
    }

    @Test
    void testComposeCGSName() {
        assertEquals("mystream-mycg", PartitionUtils.composeCGSName("mystream", "mycg"));
        assertEquals("stream1-cg2", PartitionUtils.composeCGSName("stream1", "cg2"));
    }

    @Test
    void testComposeStaticConsumerName() {
        assertEquals("mycg-member1", PartitionUtils.composeStaticConsumerName("mycg", "member1"));
        assertEquals("cg-m2", PartitionUtils.composeStaticConsumerName("cg", "m2"));
    }

    @Test
    void testGeneratePartitionFiltersBalanced() {
        // Test with 6 partitions and 3 members - should distribute evenly
        List<String> members = Arrays.asList("m1", "m2", "m3");
        int maxMembers = 6;

        List<String> m1Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m1");
        List<String> m2Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m2");
        List<String> m3Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m3");

        // Each member should get 2 partitions
        assertEquals(2, m1Filters.size());
        assertEquals(2, m2Filters.size());
        assertEquals(2, m3Filters.size());

        // m1 gets partitions 0, 1
        assertTrue(m1Filters.contains("0.>"));
        assertTrue(m1Filters.contains("1.>"));

        // m2 gets partitions 2, 3
        assertTrue(m2Filters.contains("2.>"));
        assertTrue(m2Filters.contains("3.>"));

        // m3 gets partitions 4, 5
        assertTrue(m3Filters.contains("4.>"));
        assertTrue(m3Filters.contains("5.>"));
    }

    @Test
    void testGeneratePartitionFiltersUneven() {
        // Test with 7 partitions and 3 members - remainder goes to first member
        List<String> members = Arrays.asList("m1", "m2", "m3");
        int maxMembers = 7;

        List<String> m1Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m1");
        List<String> m2Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m2");
        List<String> m3Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m3");

        // m1 gets 3 partitions (0, 1, 6)
        assertEquals(3, m1Filters.size());
        assertTrue(m1Filters.contains("0.>"));
        assertTrue(m1Filters.contains("1.>"));
        assertTrue(m1Filters.contains("6.>"));

        // m2 gets 2 partitions (2, 3)
        assertEquals(2, m2Filters.size());
        assertTrue(m2Filters.contains("2.>"));
        assertTrue(m2Filters.contains("3.>"));

        // m3 gets 2 partitions (4, 5)
        assertEquals(2, m3Filters.size());
        assertTrue(m3Filters.contains("4.>"));
        assertTrue(m3Filters.contains("5.>"));
    }

    @Test
    void testGeneratePartitionFiltersMemberMappings() {
        // Test with explicit member mappings
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 2, 4}),
                new MemberMapping("bob", new int[]{1, 3, 5})
        );

        List<String> aliceFilters = PartitionUtils.generatePartitionFilters(null, 6, mappings, "alice");
        List<String> bobFilters = PartitionUtils.generatePartitionFilters(null, 6, mappings, "bob");
        List<String> charlieFilters = PartitionUtils.generatePartitionFilters(null, 6, mappings, "charlie");

        assertEquals(3, aliceFilters.size());
        assertTrue(aliceFilters.contains("0.>"));
        assertTrue(aliceFilters.contains("2.>"));
        assertTrue(aliceFilters.contains("4.>"));

        assertEquals(3, bobFilters.size());
        assertTrue(bobFilters.contains("1.>"));
        assertTrue(bobFilters.contains("3.>"));
        assertTrue(bobFilters.contains("5.>"));

        // Non-member gets no partitions
        assertTrue(charlieFilters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersDeduplicates() {
        // Test that duplicate members are deduplicated
        List<String> members = Arrays.asList("m1", "m2", "m1", "m2", "m3");
        int maxMembers = 6;

        List<String> m1Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m1");
        List<String> m2Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m2");
        List<String> m3Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m3");

        // After deduplication, should be same as 3 unique members
        assertEquals(2, m1Filters.size());
        assertEquals(2, m2Filters.size());
        assertEquals(2, m3Filters.size());
    }

    @Test
    void testGeneratePartitionFiltersSortsMembers() {
        // Test that members are sorted before distribution
        // Even if passed in different order, same member should get same partitions
        List<String> members1 = Arrays.asList("m3", "m1", "m2");
        List<String> members2 = Arrays.asList("m1", "m2", "m3");
        int maxMembers = 6;

        List<String> m1Filters1 = PartitionUtils.generatePartitionFilters(members1, maxMembers, null, "m1");
        List<String> m1Filters2 = PartitionUtils.generatePartitionFilters(members2, maxMembers, null, "m1");

        // Should get same partitions regardless of input order
        assertEquals(m1Filters1, m1Filters2);
    }

    @Test
    void testGeneratePartitionFiltersCapToMaxMembers() {
        // Test that members are capped to maxMembers
        List<String> members = Arrays.asList("m1", "m2", "m3", "m4", "m5");
        int maxMembers = 3;

        // After sorting: m1, m2, m3, m4, m5
        // Capped to first 3: m1, m2, m3
        // m4 and m5 should not get any partitions

        List<String> m1Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m1");
        List<String> m4Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m4");
        List<String> m5Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m5");

        assertEquals(1, m1Filters.size());
        assertTrue(m4Filters.isEmpty());
        assertTrue(m5Filters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersEmptyMembers() {
        List<String> filters = PartitionUtils.generatePartitionFilters(Collections.emptyList(), 6, null, "m1");
        assertTrue(filters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersNullMembers() {
        List<String> filters = PartitionUtils.generatePartitionFilters(null, 6, null, "m1");
        assertTrue(filters.isEmpty());
    }

    @Test
    void testGeneratePartitionFiltersSingleMember() {
        List<String> members = Collections.singletonList("m1");
        int maxMembers = 4;

        List<String> m1Filters = PartitionUtils.generatePartitionFilters(members, maxMembers, null, "m1");

        // Single member should get all partitions
        assertEquals(4, m1Filters.size());
        assertTrue(m1Filters.contains("0.>"));
        assertTrue(m1Filters.contains("1.>"));
        assertTrue(m1Filters.contains("2.>"));
        assertTrue(m1Filters.contains("3.>"));
    }

    @Test
    void testDeduplicateStringList() {
        List<String> input = Arrays.asList("a", "b", "a", "c", "b");
        List<String> result = PartitionUtils.deduplicateStringList(input);

        assertEquals(3, result.size());
        assertEquals("a", result.get(0));
        assertEquals("b", result.get(1));
        assertEquals("c", result.get(2));
    }

    @Test
    void testDeduplicateStringListNull() {
        List<String> result = PartitionUtils.deduplicateStringList(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void testCalculatePullExpiry() {
        // 10 second ack wait -> 5 second pull expiry
        Duration ackWait = Duration.ofSeconds(10);
        Duration pullExpiry = PartitionUtils.calculatePullExpiry(ackWait);
        assertEquals(Duration.ofSeconds(5), pullExpiry);

        // 1 second ack wait -> 1 second pull expiry (min is 1 second)
        ackWait = Duration.ofSeconds(1);
        pullExpiry = PartitionUtils.calculatePullExpiry(ackWait);
        assertEquals(Duration.ofSeconds(1), pullExpiry);

        // 500ms ack wait -> 1 second pull expiry (min is 1 second)
        ackWait = Duration.ofMillis(500);
        pullExpiry = PartitionUtils.calculatePullExpiry(ackWait);
        assertEquals(Duration.ofSeconds(1), pullExpiry);
    }

    @Test
    void testCalculatePinnedTTL() {
        // 10 second ack wait -> 10 second pinned TTL
        Duration ackWait = Duration.ofSeconds(10);
        Duration pinnedTTL = PartitionUtils.calculatePinnedTTL(ackWait);
        assertEquals(Duration.ofSeconds(10), pinnedTTL);

        // 500ms ack wait -> 1 second pinned TTL (min is 1 second)
        ackWait = Duration.ofMillis(500);
        pinnedTTL = PartitionUtils.calculatePinnedTTL(ackWait);
        assertEquals(Duration.ofSeconds(1), pinnedTTL);
    }

    @Test
    void testCalculateInactiveThreshold() {
        Duration ackWait = Duration.ofSeconds(5);
        Duration threshold = PartitionUtils.calculateInactiveThreshold(ackWait);
        assertEquals(Duration.ofSeconds(5), threshold);
    }

    @Test
    void testConstants() {
        assertEquals(2, PartitionUtils.PULL_TIMEOUT_DIVIDER);
        assertEquals(1, PartitionUtils.CONSUMER_IDLE_TIMEOUT_FACTOR);
        assertEquals(Duration.ofSeconds(5), PartitionUtils.DEFAULT_ACK_WAIT);
        assertEquals(Duration.ofSeconds(1), PartitionUtils.MIN_PULL_EXPIRY_PINNED_TTL);
        assertEquals("PCG", PartitionUtils.PRIORITY_GROUP_NAME);
        assertEquals("static-consumer-groups", PartitionUtils.KV_STATIC_BUCKET_NAME);
        assertEquals("elastic-consumer-groups", PartitionUtils.KV_ELASTIC_BUCKET_NAME);
    }
}
