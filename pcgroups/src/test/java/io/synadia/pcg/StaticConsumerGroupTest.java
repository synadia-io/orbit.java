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
 * Unit tests for StaticConsumerGroupConfig.
 * Tests config validation and JSON serialization compatibility with Go.
 */
class StaticConsumerGroupTest {

    static {
        NatsRunnerUtils.setDefaultOutputLevel(Level.SEVERE);
    }

    @Test
    void testConfigWithMembers() {
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2", "m3", "m4"),
                new ArrayList<>()
        );

        assertEquals(4, config.getMaxMembers());
        assertEquals("foo.>", config.getFilter());
        assertEquals(4, config.getMembers().size());
        assertTrue(config.getMemberMappings().isEmpty());

        assertTrue(config.isInMembership("m1"));
        assertTrue(config.isInMembership("m4"));
        assertFalse(config.isInMembership("m5"));
    }

    @Test
    void testConfigWithMemberMappings() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        assertEquals(4, config.getMaxMembers());
        assertTrue(config.getMembers().isEmpty());
        assertEquals(2, config.getMemberMappings().size());

        assertTrue(config.isInMembership("alice"));
        assertTrue(config.isInMembership("bob"));
        assertFalse(config.isInMembership("charlie"));
    }

    @Test
    void testValidationMaxMembersZero() {
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                0, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("max number of members must be >= 1"));
    }

    @Test
    void testValidationBothMembersAndMappings() {
        List<MemberMapping> mappings = Collections.singletonList(
                new MemberMapping("alice", new int[]{0, 1})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                2, "foo.>",
                Arrays.asList("m1", "m2"),
                mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("either members or member mappings must be provided, not both"));
    }

    @Test
    void testValidationDuplicateMemberNames() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("alice", new int[]{2, 3})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("member names must be unique"));
    }

    @Test
    void testValidationDuplicatePartitions() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{1, 2})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                3, "foo.>", new ArrayList<>(), mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("partition numbers must be used only once"));
    }

    @Test
    void testValidationPartitionOutOfRange() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 5})  // 5 is out of range for maxMembers=4
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("partition numbers must be between 0 and one less"));
    }

    @Test
    void testValidationNotAllPartitionsCovered() {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2})  // Missing partition 3
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        ConsumerGroupException exception = assertThrows(ConsumerGroupException.class, config::validate);
        assertTrue(exception.getMessage().contains("number of unique partition numbers must be equal"));
    }

    @Test
    void testValidationSuccess() throws ConsumerGroupException {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    void testValidationWithMembersSuccess() throws ConsumerGroupException {
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2", "m3", "m4"),
                new ArrayList<>()
        );

        assertDoesNotThrow(config::validate);
    }

    @Test
    void testJsonSerializationWithMembers() throws JsonParseException {
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        String json = config.toJson();

        // Verify JSON structure matches Go format
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"filter\":\"foo.>\""));
        assertTrue(json.contains("\"members\":[\"m1\",\"m2\"]"));

        // Deserialize and verify
        StaticConsumerGroupConfig deserialized = StaticConsumerGroupConfig.instance(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(config.getMaxMembers(), deserialized.getMaxMembers());
        assertEquals(config.getFilter(), deserialized.getFilter());
        assertEquals(config.getMembers(), deserialized.getMembers());
    }

    @Test
    void testJsonSerializationWithMappings() throws JsonParseException {
        List<MemberMapping> mappings = Arrays.asList(
                new MemberMapping("alice", new int[]{0, 1}),
                new MemberMapping("bob", new int[]{2, 3})
        );

        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(
                4, "foo.>", new ArrayList<>(), mappings
        );

        String json = config.toJson();

        // Verify JSON structure matches Go format
        assertTrue(json.contains("\"max_members\":4"));
        assertTrue(json.contains("\"member_mappings\""));
        assertTrue(json.contains("\"member\":\"alice\""));
        assertTrue(json.contains("\"partitions\":[0,1]"));

        // Deserialize and verify
        StaticConsumerGroupConfig deserialized = StaticConsumerGroupConfig.instance(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(config.getMaxMembers(), deserialized.getMaxMembers());
        assertEquals(2, deserialized.getMemberMappings().size());
        assertEquals("alice", deserialized.getMemberMappings().get(0).getMember());
        assertArrayEquals(new int[]{0, 1}, deserialized.getMemberMappings().get(0).getPartitions());
    }

    @Test
    void testJsonDeserializationFromGo() throws JsonParseException {
        // This JSON is in the format produced by the Go implementation
        String goJson = "{\"max_members\":4,\"filter\":\"foo.>\",\"members\":[\"m1\",\"m2\",\"m3\",\"m4\"]}";

        StaticConsumerGroupConfig config = StaticConsumerGroupConfig.instance(goJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(4, config.getMaxMembers());
        assertEquals("foo.>", config.getFilter());
        assertEquals(4, config.getMembers().size());
        assertEquals("m1", config.getMembers().get(0));
    }

    @Test
    void testJsonDeserializationWithMappingsFromGo() throws JsonParseException {
        // This JSON is in the format produced by the Go implementation
        String goJson = "{\"max_members\":4,\"filter\":\"bar.>\",\"member_mappings\":[{\"member\":\"alice\",\"partitions\":[0,1]},{\"member\":\"bob\",\"partitions\":[2,3]}]}";

        StaticConsumerGroupConfig config = StaticConsumerGroupConfig.instance(goJson.getBytes(StandardCharsets.UTF_8));

        assertEquals(4, config.getMaxMembers());
        assertEquals("bar.>", config.getFilter());
        assertEquals(2, config.getMemberMappings().size());
        assertEquals("alice", config.getMemberMappings().get(0).getMember());
        assertArrayEquals(new int[]{0, 1}, config.getMemberMappings().get(0).getPartitions());
        assertEquals("bob", config.getMemberMappings().get(1).getMember());
        assertArrayEquals(new int[]{2, 3}, config.getMemberMappings().get(1).getPartitions());
    }

    @Test
    void testEquals() {
        StaticConsumerGroupConfig config1 = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        StaticConsumerGroupConfig config2 = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        StaticConsumerGroupConfig config3 = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m3"),
                new ArrayList<>()
        );

        assertEquals(config1, config2);
        assertNotEquals(config1, config3);
    }

    @Test
    void testHashCode() {
        StaticConsumerGroupConfig config1 = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        StaticConsumerGroupConfig config2 = new StaticConsumerGroupConfig(
                4, "foo.>",
                Arrays.asList("m1", "m2"),
                new ArrayList<>()
        );

        assertEquals(config1.hashCode(), config2.hashCode());
    }
}
