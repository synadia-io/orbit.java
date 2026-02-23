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

import io.nats.NatsRunnerUtils;
import io.nats.client.support.JsonParseException;
import io.synadia.pcg.exceptions.ConsumerGroupException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ElasticConsumerGroupConfig.
 * Tests config validation, JSON serialization, and partition transform destination generation.
 */
class ElasticConsumerGroupTest {

    static {
        NatsRunnerUtils.setDefaultOutputLevel(Level.SEVERE);
    }

    @Test
    void testConfigBasic() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        assertEquals(4, config.getMaxMembers());
        assertEquals("foo.*", config.getFilter());
        assertArrayEquals(new int[]{1}, config.getPartitioningWildcards());
        assertEquals(1000, config.getMaxBufferedMessages());
        assertEquals(10000, config.getMaxBufferedBytes());
        assertEquals(2, config.getMembers().size());
        assertTrue(config.getMemberMappings().isEmpty());
    }

    @Test
    void testIsInMembership() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2", "m3"), new ArrayList<>()
        );

        assertTrue(config.isInMembership("m1"));
        assertTrue(config.isInMembership("m2"));
        assertTrue(config.isInMembership("m3"));
        assertFalse(config.isInMembership("m4"));
    }

    @Test
    void testIsInMembershipWithMappings() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                new ArrayList<>(), mappings
        );

        assertTrue(config.isInMembership("alice"));
        assertTrue(config.isInMembership("bob"));
        assertFalse(config.isInMembership("charlie"));
    }

    @Test
    void testValidationMaxMembersZero() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                0, "foo.*", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("max number of members must be >= 1"));
    }

    @Test
    void testValidationFilterNoWildcard() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.bar", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("filter must contain at least one * wildcard"));
    }

    @Test
    void testValidationPartitioningWildcardsEmpty() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("number of partitioning wildcards must be between"));
    }

    @Test
    void testValidationPartitioningWildcardsTooMany() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1, 2}, 0, 0,  // Only 1 wildcard in filter
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("number of partitioning wildcards must be between"));
    }

    @Test
    void testValidationPartitioningWildcardsOutOfRange() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{2}, 0, 0,  // Index 2 is out of range (only 1 wildcard)
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("partitioning wildcard indexes must be greater than 1"));
    }

    @Test
    void testValidationPartitioningWildcardsDuplicate() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*.bar.*", new int[]{1, 1}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("partitioning wildcard indexes must be unique"));
    }

    @Test
    void testValidationBothMembersAndMappings() {
        List<MemberMapping> mappings = Collections.singletonList(
                new MemberMapping("alice", new int[]{0, 1, 2, 3})
        );

        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2"), mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("either members or member mappings must be provided, not both"));
    }

    @Test
    void testValidationSuccess() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    void testValidationSuccessWithMappings() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                new ArrayList<>(), mappings
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    void testValidationSuccessMultipleWildcards() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*.bar.*", new int[]{1, 2}, 0, 0,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    void testGetPartitioningTransformDestSingle() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                new ArrayList<>(), new ArrayList<>()
        );

        String dest = config.getPartitioningTransformDest();
        assertEquals("{{Partition(4,1)}}.foo.{{Wildcard(1)}}", dest);
    }

    @Test
    void testGetPartitioningTransformDestMultiple() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                6, "foo.*.bar.*", new int[]{1, 2}, 0, 0,
                new ArrayList<>(), new ArrayList<>()
        );

        String dest = config.getPartitioningTransformDest();
        assertEquals("{{Partition(6,1,2)}}.foo.{{Wildcard(1)}}.bar.{{Wildcard(2)}}", dest);
    }

    @Test
    void testGetPartitioningTransformDestPartialWildcards() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                8, "a.*.b.*.c.*", new int[]{2}, 0, 0,
                new ArrayList<>(), new ArrayList<>()
        );

        String dest = config.getPartitioningTransformDest();
        assertEquals("{{Partition(8,2)}}.a.{{Wildcard(1)}}.b.{{Wildcard(2)}}.c.{{Wildcard(3)}}", dest);
    }

    @Test
    void testJsonSerializationWithMembers() throws JsonParseException {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        String json = config.toJson();

        // Verify JSON structure matches Go format
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"foo.*\""));
        assertTrue(json.contains("\"partitioning_wildcards\":[1]"));
        assertTrue(json.contains("\"max_buffered_msg\":1000"));
        assertTrue(json.contains("\"max_buffered_bytes\":10000"));
        assertTrue(json.contains("\"members\":[\"m1\",\"m2\"]"));

        // Deserialize and verify
        ElasticConsumerGroupConfig deserialized = ElasticConsumerGroupConfig.instance(config.serialize());
        assertEquals(config.getMaxMembers(), deserialized.getMaxMembers());
        assertEquals(config.getFilter(), deserialized.getFilter());
        assertArrayEquals(config.getPartitioningWildcards(), deserialized.getPartitioningWildcards());
        assertEquals(config.getMaxBufferedMessages(), deserialized.getMaxBufferedMessages());
        assertEquals(config.getMaxBufferedBytes(), deserialized.getMaxBufferedBytes());
        assertEquals(config.getMembers(), deserialized.getMembers());
    }

    @Test
    void testJsonSerializationWithMappings() throws JsonParseException {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 0, 0,
                new ArrayList<>(), mappings
        );

        String json = config.toJson();

        assertTrue(json.contains("\"member_mappings\""));
        assertTrue(json.contains("\"member\":\"alice\""));
        assertTrue(json.contains("\"partitions\":[0,1]"));

        // Deserialize and verify
        ElasticConsumerGroupConfig deserialized = ElasticConsumerGroupConfig.instance(config.serialize());
        assertEquals(2, deserialized.getMemberMappings().size());
        assertEquals("alice", deserialized.getMemberMappings().get(0).getMember());
        assertArrayEquals(new int[]{0, 1}, deserialized.getMemberMappings().get(0).getPartitions());
    }

    @Test
    void testJsonDeserializationFromGo() throws JsonParseException {
        // This JSON is in the format produced by the Go implementation
        String goJson = "{\"max_members\":4,\"filter\":\"foo.*\",\"partitioning_wildcards\":[1],\"max_buffered_msg\":1000,\"max_buffered_bytes\":10000,\"members\":[\"m1\",\"m2\"]}";

        ElasticConsumerGroupConfig config = ElasticConsumerGroupConfig.instance(goJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(4, config.getMaxMembers());
        assertEquals("foo.*", config.getFilter());
        assertArrayEquals(new int[]{1}, config.getPartitioningWildcards());
        assertEquals(1000, config.getMaxBufferedMessages());
        assertEquals(10000, config.getMaxBufferedBytes());
        assertEquals(2, config.getMembers().size());
        assertEquals("m1", config.getMembers().get(0));
        assertEquals("m2", config.getMembers().get(1));
    }

    @Test
    void testJsonDeserializationWithMappingsFromGo() throws JsonParseException {
        // This JSON is in the format produced by the Go implementation
        String goJson = "{\"max_members\":4,\"filter\":\"bar.*\",\"partitioning_wildcards\":[1],\"member_mappings\":[{\"member\":\"alice\",\"partitions\":[0,1]},{\"member\":\"bob\",\"partitions\":[2,3]}]}";

        ElasticConsumerGroupConfig config = ElasticConsumerGroupConfig.instance(goJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(4, config.getMaxMembers());
        assertEquals("bar.*", config.getFilter());
        assertArrayEquals(new int[]{1}, config.getPartitioningWildcards());
        assertEquals(2, config.getMemberMappings().size());
        assertEquals("alice", config.getMemberMappings().get(0).getMember());
        assertArrayEquals(new int[]{0, 1}, config.getMemberMappings().get(0).getPartitions());
        assertEquals("bob", config.getMemberMappings().get(1).getMember());
        assertArrayEquals(new int[]{2, 3}, config.getMemberMappings().get(1).getPartitions());
    }

    @Test
    void testEquals() {
        ElasticConsumerGroupConfig config1 = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ElasticConsumerGroupConfig config2 = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ElasticConsumerGroupConfig config3 = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m3"), new ArrayList<>()
        );

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
    }

    @Test
    void testHashCode() {
        ElasticConsumerGroupConfig config1 = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        ElasticConsumerGroupConfig config2 = new ElasticConsumerGroupConfig(
                4, "foo.*", new int[]{1}, 1000, 10000,
                Arrays.asList("m1", "m2"), new ArrayList<>()
        );

        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    void testRevision() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig();
        assertEquals(0, config.getRevision());

        config.setRevision(42);
        assertEquals(42, config.getRevision());

        // Verify revision is not serialized
        String json = config.toJson();
        assertFalse(json.contains("revision"));
    }

    @Test
    void testGetPartitionFilters() {
        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                6, "foo.*", new int[]{1}, 0, 0,
                Arrays.asList("m1", "m2", "m3"), new ArrayList<>()
        );

        List<String> m1Filters = ElasticConsumerGroup.getPartitionFilters(config, "m1");
        List<String> m2Filters = ElasticConsumerGroup.getPartitionFilters(config, "m2");
        List<String> m3Filters = ElasticConsumerGroup.getPartitionFilters(config, "m3");

        // Each member should get 2 partitions
        assertEquals(2, m1Filters.size());
        assertEquals(2, m2Filters.size());
        assertEquals(2, m3Filters.size());

        // Verify distribution
        assertTrue(m1Filters.contains("0.>"));
        assertTrue(m1Filters.contains("1.>"));
        assertTrue(m2Filters.contains("2.>"));
        assertTrue(m2Filters.contains("3.>"));
        assertTrue(m3Filters.contains("4.>"));
        assertTrue(m3Filters.contains("5.>"));
    }
}
