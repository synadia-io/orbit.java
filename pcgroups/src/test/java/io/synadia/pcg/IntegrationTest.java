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

import io.nats.NatsServerRunner;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.SubjectTransform;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Static and Elastic consumer groups.
 * Ported from Go: golang/test/stream_consumer_group_test.go
 */
class IntegrationTest {

    /**
     * Ported from Go TestStatic.
     * Creates a stream with subject transform, publishes 10 messages,
     * and verifies 2 static members consume all messages.
     */
    @Test
    void testStatic() throws Exception {
        try (NatsServerRunner server = NatsServerRunner.builder().jetstream(true).build()) {
            Connection nc = Nats.connect(server.getNatsLocalhostUri());

            String streamName = "test";
            String cgName = "group";
            AtomicInteger c1 = new AtomicInteger(0);
            AtomicInteger c2 = new AtomicInteger(0);

            // Create a stream with subject transform (like the Go test)
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name(streamName)
                    .subjects("bar.*")
                    .subjectTransform(SubjectTransform.builder()
                            .source("bar.*")
                            .destination("{{partition(2,1)}}.bar.{{wildcard(1)}}")
                            .build())
                    .storageType(StorageType.Memory)
                    .build());

            // Publish 10 messages
            for (int i = 0; i < 10; i++) {
                nc.jetStream().publish("bar." + i, "payload".getBytes());
            }

            ConsumerConfiguration config = ConsumerConfiguration.builder()
                    .maxAckPending(1)
                    .ackWait(Duration.ofSeconds(1))
                    .build();

            // Create static consumer group
            StaticConsumerGroup.create(nc, streamName, cgName, 2, "bar.*",
                    Arrays.asList("m1", "m2"), new ArrayList<>());

            // Start consuming on both members
            ConsumerGroupConsumeContext cc1 = StaticConsumerGroup.consume(nc, streamName, cgName, "m1", msg -> {
                c1.incrementAndGet();
                try {
                    msg.ack();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, config);

            ConsumerGroupConsumeContext cc2 = StaticConsumerGroup.consume(nc, streamName, cgName, "m2", msg -> {
                c2.incrementAndGet();
                try {
                    msg.ack();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, config);

            // Wait for all 10 messages to be consumed
            long deadline = System.currentTimeMillis() + 5000;
            while (c1.get() + c2.get() < 10) {
                Thread.sleep(100);
                if (System.currentTimeMillis() > deadline) {
                    fail("timeout: c1=" + c1.get() + " c2=" + c2.get() + " expected total=10");
                }
            }

            assertEquals(10, c1.get() + c2.get());

            cc1.stop();
            cc2.stop();

            // Delete static consumer group
            StaticConsumerGroup.delete(nc, streamName, cgName);
            nc.close();
        }
    }

    /**
     * Ported from Go TestElastic.
     * Creates a stream, tests elastic consume with member add/delete.
     * Verifies partition redistribution when members are added and removed.
     */
    @Test
    void testElastic() throws Exception {
        try (NatsServerRunner server = NatsServerRunner.builder().jetstream(true).build()) {
            Connection nc = Nats.connect(server.getNatsLocalhostUri());

            String streamName = "test";
            String cgName = "group";
            AtomicInteger c1 = new AtomicInteger(0);
            AtomicInteger c2 = new AtomicInteger(0);

            // Create a stream (no subject transform needed for elastic)
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name(streamName)
                    .subjects("bar.*")
                    .storageType(StorageType.Memory)
                    .build());

            // Publish 10 messages
            for (int i = 0; i < 10; i++) {
                nc.jetStream().publish("bar." + i, "payload".getBytes());
            }

            ConsumerConfiguration config = ConsumerConfiguration.builder()
                    .maxAckPending(1)
                    .ackWait(Duration.ofSeconds(1))
                    .build();

            // Create elastic consumer group
            ElasticConsumerGroup.create(nc, streamName, cgName, 2, "bar.*",
                    new int[]{1}, -1, -1);

            // Start consuming on both members
            ConsumerGroupConsumeContext cc1 = ElasticConsumerGroup.consume(nc, streamName, cgName, "m1", msg -> {
                c1.incrementAndGet();
                try {
                    msg.ack();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, config);

            ConsumerGroupConsumeContext cc2 = ElasticConsumerGroup.consume(nc, streamName, cgName, "m2", msg -> {
                c2.incrementAndGet();
                try {
                    msg.ack();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, config);

            // Wait for KV watchers to be established on the instance threads
            Thread.sleep(200);

            // Add only m1 first
            ElasticConsumerGroup.addMembers(nc, streamName, cgName, Collections.singletonList("m1"));

            // Wait for m1 to consume all 10 messages (only m1 is a member)
            long deadline = System.currentTimeMillis() + 5000;
            while (c1.get() != 10 || c2.get() != 0) {
                Thread.sleep(100);
                if (System.currentTimeMillis() > deadline) {
                    fail("timeout phase 1: c1=" + c1.get() + " c2=" + c2.get() + " expected c1=10 c2=0");
                }
            }
            assertEquals(10, c1.get());
            assertEquals(0, c2.get());

            // Add m2
            ElasticConsumerGroup.addMembers(nc, streamName, cgName, Collections.singletonList("m2"));

            // Wait for consumers to be recreated
            Thread.sleep(50);

            // Publish 10 more messages
            for (int i = 0; i < 10; i++) {
                nc.jetStream().publish("bar." + i, "payload".getBytes());
            }

            // Wait for split consumption (m1=15, m2=5)
            deadline = System.currentTimeMillis() + 10000;
            while (c1.get() != 15 || c2.get() != 5) {
                Thread.sleep(100);
                if (System.currentTimeMillis() > deadline) {
                    fail("timeout phase 2: c1=" + c1.get() + " c2=" + c2.get() + " expected c1=15 c2=5");
                }
            }

            // Delete m1
            ElasticConsumerGroup.deleteMembers(nc, streamName, cgName, Collections.singletonList("m1"));

            // Wait for consumers to be recreated
            Thread.sleep(50);

            // Publish 10 more messages
            for (int i = 0; i < 10; i++) {
                nc.jetStream().publish("bar." + i, "payload".getBytes());
            }

            // Wait for m2 to consume all (m1=15 stays, m2=15)
            deadline = System.currentTimeMillis() + 10000;
            while (c1.get() != 15 || c2.get() != 15) {
                Thread.sleep(100);
                if (System.currentTimeMillis() > deadline) {
                    fail("timeout phase 3: c1=" + c1.get() + " c2=" + c2.get() + " expected c1=15 c2=15");
                }
            }

            cc1.stop();
            cc2.stop();

            // Delete elastic consumer group
            ElasticConsumerGroup.delete(nc, streamName, cgName);
            nc.close();
        }
    }
}
