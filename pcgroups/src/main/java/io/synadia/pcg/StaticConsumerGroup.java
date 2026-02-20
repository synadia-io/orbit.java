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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.pcg.exceptions.ConsumerGroupException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static io.synadia.pcg.PartitionUtils.*;

/**
 * Static consumer group implementation.
 * Provides static partition assignment for consumer groups.
 */
public class StaticConsumerGroup {

    private static final Logger LOGGER = Logger.getLogger(StaticConsumerGroup.class.getName());
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private StaticConsumerGroup() {
        // Utility class
    }

    /**
     * Creates a static consumer group.
     *
     * @param nc                NATS connection
     * @param streamName        Name of the stream
     * @param consumerGroupName Name of the consumer group
     * @param maxMembers        Maximum number of members (partitions)
     * @param filter            Subject filter
     * @param members           List of member names (for balanced distribution)
     * @param memberMappings    Explicit member-to-partition mappings
     * @return The created configuration
     */
    public static StaticConsumerGroupConfig create(Connection nc, String streamName, String consumerGroupName,
                                                   int maxMembers, String filter, List<String> members,
                                                   List<MemberMapping> memberMappings) throws ConsumerGroupException, IOException, JetStreamApiException, InterruptedException {
        StaticConsumerGroupConfig config = new StaticConsumerGroupConfig(maxMembers, filter, members, memberMappings);
        config.validate();

        JetStreamManagement jsm = nc.jetStreamManagement();

        // Get stream info to determine replicas
        StreamInfo streamInfo = jsm.getStreamInfo(streamName);
        int replicas = streamInfo.getConfiguration().getReplicas();

        // Get or create the KV bucket
        KeyValueManagement kvm = nc.keyValueManagement();
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            // Create the bucket if it doesn't exist
            KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                    .name(KV_STATIC_BUCKET_NAME)
                    .replicas(replicas)
                    .storageType(StorageType.File)
                    .build();
            kvm.create(kvConfig);
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        }

        String key = composeKey(streamName, consumerGroupName);

        // Check if config already exists
        KeyValueEntry entry = kv.get(key);
        if (entry != null) {
            String json = new String(entry.getValue(), StandardCharsets.UTF_8);
            StaticConsumerGroupConfig existingConfig = GSON.fromJson(json, StaticConsumerGroupConfig.class);

            // Verify the config matches
            if (!configsMatch(existingConfig, config)) {
                throw new ConsumerGroupException("the existing static consumer group config doesn't match ours");
            }
            return existingConfig;
        }

        // Create the config entry
        String payload = GSON.toJson(config);
        kv.put(key, payload.getBytes(StandardCharsets.UTF_8));

        return config;
    }

    /**
     * Starts consuming messages from a static consumer group.
     *
     * @param nc                NATS connection
     * @param streamName        Name of the stream
     * @param consumerGroupName Name of the consumer group
     * @param memberName        Name of this member
     * @param handler           Message handler callback
     * @param consumerConfig    Consumer configuration (null for defaults)
     * @return A consume context for controlling the consumption lifecycle
     */
    public static ConsumerGroupConsumeContext consume(Connection nc, String streamName, String consumerGroupName,
                                                      String memberName, Consumer<ConsumerGroupMsg> handler,
                                                      ConsumerConfiguration consumerConfig) throws ConsumerGroupException, IOException, JetStreamApiException, InterruptedException {
        if (handler == null) {
            throw new ConsumerGroupException("a message handler must be provided");
        }

        if (consumerConfig == null) {
            consumerConfig = ConsumerConfiguration.builder().ackWait(DEFAULT_ACK_WAIT).build();
        } else if (consumerConfig.getAckWait() == null || consumerConfig.getAckWait().isZero() || consumerConfig.getAckWait().isNegative()) {
            consumerConfig = ConsumerConfiguration.builder(consumerConfig).ackWait(DEFAULT_ACK_WAIT).build();
        }

        // Get the KV bucket
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the static consumer group KV bucket doesn't exist", e);
        }

        // Get the config
        StaticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        if (!config.isInMembership(memberName)) {
            throw new ConsumerGroupException("the member name is not in the current static consumer group membership");
        }

        // Verify stream exists
        JetStreamManagement jsm = nc.jetStreamManagement();
        try {
            jsm.getStreamInfo(streamName);
        } catch (JetStreamApiException e) {
            throw new ConsumerGroupException("the static consumer group's stream does not exist", e);
        }

        return new StaticConsumeContextImpl(nc, kv, streamName, consumerGroupName, memberName, config, handler, consumerConfig);
    }

    /**
     * Deletes a static consumer group.
     */
    public static void delete(Connection nc, String streamName, String consumerGroupName) throws IOException, JetStreamApiException, InterruptedException {
        JetStreamManagement jsm = nc.jetStreamManagement();

        // Get the KV bucket
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            return; // Bucket doesn't exist
        }

        // Delete the config entry
        String key = composeKey(streamName, consumerGroupName);
        try {
            kv.delete(key);
        } catch (Exception e) {
            // Ignore if key doesn't exist
        }

        // Delete consumers that match the pattern
        List<String> consumerNames = jsm.getConsumerNames(streamName);

        for (String consumerName : consumerNames) {
            if (consumerName.startsWith(consumerGroupName + "-")) {
                try {
                    jsm.deleteConsumer(streamName, consumerName);
                } catch (JetStreamApiException e) {
                    // Ignore if consumer doesn't exist
                }
            }
        }
    }

    /**
     * Lists static consumer groups for a stream.
     */
    public static List<String> list(Connection nc, String streamName) throws IOException, JetStreamApiException, InterruptedException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            return new ArrayList<>();
        }

        List<String> keys = kv.keys();
        List<String> consumerGroupNames = new ArrayList<>();

        for (String key : keys) {
            String[] parts = key.split("\\.");
            if (parts.length >= 2 && parts[0].equals(streamName)) {
                consumerGroupNames.add(parts[1]);
            }
        }

        return consumerGroupNames;
    }

    /**
     * Lists active members of a static consumer group.
     */
    public static List<String> listActiveMembers(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the static consumer group KV bucket doesn't exist", e);
        }

        StaticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);
        JetStreamManagement jsm = nc.jetStreamManagement();

        List<String> activeMembers = new ArrayList<>();
        List<ConsumerInfo> consumers = jsm.getConsumers(streamName);

        List<String> memberList = config.getMembers();
        List<MemberMapping> mappings = config.getMemberMappings();

        for (ConsumerInfo cInfo : consumers) {
            if (!memberList.isEmpty()) {
                for (String m : memberList) {
                    if (cInfo.getName().equals(composeStaticConsumerName(consumerGroupName, m)) && cInfo.getNumWaiting() > 0) {
                        activeMembers.add(m);
                        break;
                    }
                }
            } else if (!mappings.isEmpty()) {
                for (MemberMapping mapping : mappings) {
                    if (cInfo.getName().equals(composeStaticConsumerName(consumerGroupName, mapping.getMember())) && cInfo.getNumWaiting() > 0) {
                        activeMembers.add(mapping.getMember());
                        break;
                    }
                }
            }
        }

        return activeMembers;
    }

    /**
     * Forces the current active (pinned) instance for a member to step down.
     * This requires NATS server 2.11+ with priority consumer support.
     */
    public static void memberStepDown(Connection nc, String streamName, String consumerGroupName, String memberName)
            throws IOException, JetStreamApiException, InterruptedException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        String consumerName = composeStaticConsumerName(consumerGroupName, memberName);
        jsm.unpinConsumer(streamName, consumerName, PRIORITY_GROUP_NAME);
    }

    /**
     * Gets the static consumer group configuration.
     */
    public static StaticConsumerGroupConfig getConfig(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_STATIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the static consumer group KV bucket doesn't exist", e);
        }

        return getConfigFromKV(kv, streamName, consumerGroupName);
    }

    private static StaticConsumerGroupConfig getConfigFromKV(KeyValue kv, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() || consumerGroupName == null || consumerGroupName.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or consumer group name");
        }

        String key = composeKey(streamName, consumerGroupName);
        KeyValueEntry entry = kv.get(key);

        if (entry == null) {
            throw new ConsumerGroupException("error getting the static consumer group's config: not found");
        }

        String json = new String(entry.getValue(), StandardCharsets.UTF_8);
        StaticConsumerGroupConfig config = GSON.fromJson(json, StaticConsumerGroupConfig.class);
        config.validate();

        return config;
    }

    private static boolean configsMatch(StaticConsumerGroupConfig a, StaticConsumerGroupConfig b) {
        return a.getMaxMembers() == b.getMaxMembers() &&
                java.util.Objects.equals(a.getFilter(), b.getFilter()) &&
                java.util.Objects.equals(a.getMembers(), b.getMembers()) &&
                java.util.Objects.equals(a.getMemberMappings(), b.getMemberMappings());
    }

    /**
     * Internal implementation of the consume context for static consumer groups.
     */
    private static class StaticConsumeContextImpl implements ConsumerGroupConsumeContext {
        private final Connection nc;
        private final KeyValue kv;
        private final String streamName;
        private final String consumerGroupName;
        private final String memberName;
        private final StaticConsumerGroupConfig config;
        private final Consumer<ConsumerGroupMsg> handler;
        private final ConsumerConfiguration consumerUserConfig;
        private final CompletableFuture<Void> doneFuture;
        private final AtomicBoolean stopped;
        private final AtomicReference<String> currentPinnedId;
        private MessageConsumer messageConsumer;
        private io.nats.client.impl.NatsKeyValueWatchSubscription watchSubscription;

        StaticConsumeContextImpl(Connection nc, KeyValue kv, String streamName, String consumerGroupName,
                                 String memberName, StaticConsumerGroupConfig config,
                                 Consumer<ConsumerGroupMsg> handler, ConsumerConfiguration consumerUserConfig)
                throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
            this.nc = nc;
            this.kv = kv;
            this.streamName = streamName;
            this.consumerGroupName = consumerGroupName;
            this.memberName = memberName;
            this.config = config;
            this.handler = handler;
            this.consumerUserConfig = consumerUserConfig;
            this.doneFuture = new CompletableFuture<>();
            this.stopped = new AtomicBoolean(false);
            this.currentPinnedId = new AtomicReference<>("");

            joinMemberConsumer();
            startWatcher();
        }

        private void joinMemberConsumer() throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
            List<String> filters = PartitionUtils.generatePartitionFilters(
                    config.getMembers(), config.getMaxMembers(), config.getMemberMappings(), memberName);

            if (filters.isEmpty()) {
                return;
            }

            String consumerName = composeStaticConsumerName(consumerGroupName, memberName);

            // Build consumer configuration from user config, overriding internal fields
            Duration pinnedTTL = calculatePinnedTTL(consumerUserConfig.getAckWait());
            ConsumerConfiguration cc = ConsumerConfiguration.builder(consumerUserConfig)
                    .durable(consumerName)
                    .name(consumerName) // Needed for cross-compatibility with Go client which creates the consumer using another JS API call
                    .filterSubjects(filters)
                    .priorityGroups(PRIORITY_GROUP_NAME)
                    .priorityPolicy(PriorityPolicy.PinnedClient)
                    .priorityTimeout(pinnedTTL)
                    .build();

            // Create the durable consumer explicitly (matching Go's js.CreateConsumer)
            JetStreamManagement jsm = nc.jetStreamManagement();
            System.out.printf("Creating consumer %s with filters %s and priority group %s%n\n", consumerName, filters, PRIORITY_GROUP_NAME);
            jsm.createConsumer(streamName, cc);

            // Get consumer context and start consuming
            StreamContext sc = nc.getStreamContext(streamName);
            ConsumerContext consumerCtx = sc.getConsumerContext(consumerName);

            Duration pullExpiry = calculatePullExpiry(consumerUserConfig.getAckWait());
            ConsumeOptions co = ConsumeOptions.builder()
                    .expiresIn(pullExpiry.toMillis())
                    .group(PRIORITY_GROUP_NAME)
                    .build();

            messageConsumer = consumerCtx.consume(co, msg -> {
                String pid = null;
                Headers headers = msg.getHeaders();
                if (headers != null) {
                    pid = headers.getFirst("Nats-Pin-Id");
                }

                if (pid != null && !pid.isEmpty()) {
                    String current = currentPinnedId.get();
                    if (current.isEmpty() || !current.equals(pid)) {
                        currentPinnedId.set(pid);
                    }
                }

                ConsumerGroupMsg cgMsg = new ConsumerGroupMsg(msg);
                handler.accept(cgMsg);
            });
        }

        private void startWatcher() {
            Thread watcherThread = new Thread(() -> {
                try {
                    String key = composeKey(streamName, consumerGroupName);
                    KeyValueWatcher watcher = new KeyValueWatcher() {
                        @Override
                        public void watch(KeyValueEntry entry) {
                            if (stopped.get()) {
                                return;
                            }

                            if (entry.getOperation() == KeyValueOperation.DELETE ||
                                entry.getOperation() == KeyValueOperation.PURGE) {
                                stopAndDeleteMemberConsumer();
                                doneFuture.complete(null);
                                return;
                            }

                            try {
                                String json = new String(entry.getValue(), StandardCharsets.UTF_8);
                                StaticConsumerGroupConfig newConfig = GSON.fromJson(json, StaticConsumerGroupConfig.class);
                                newConfig.validate();

                                // Check if critical config changed
                                if (newConfig.getMaxMembers() != config.getMaxMembers() ||
                                        !java.util.Objects.equals(newConfig.getFilter(), config.getFilter()) ||
                                        !java.util.Objects.equals(newConfig.getMembers(), config.getMembers()) ||
                                        !java.util.Objects.equals(newConfig.getMemberMappings(), config.getMemberMappings())) {
                                    stopAndDeleteMemberConsumer();
                                    doneFuture.completeExceptionally(
                                            new ConsumerGroupException("static consumer group config watcher received a change in the configuration, terminating"));
                                }
                            } catch (Exception e) {
                                stopAndDeleteMemberConsumer();
                                doneFuture.completeExceptionally(e);
                            }
                        }

                        @Override
                        public void endOfData() {
                            // Initial data load complete
                        }
                    };

                    watchSubscription = kv.watch(key, watcher, KeyValueWatchOption.UPDATES_ONLY);

                } catch (Exception e) {
                    if (!stopped.get()) {
                        doneFuture.completeExceptionally(e);
                    }
                }
            });
            watcherThread.setDaemon(true);
            watcherThread.start();
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                stopConsuming();
                doneFuture.complete(null);
            }
        }

        @Override
        public CompletableFuture<Void> done() {
            return doneFuture;
        }

        private void stopConsuming() {
            if (messageConsumer != null) {
                try {
                    messageConsumer.close();
                } catch (Exception e) {
                    // Ignore
                }
                messageConsumer = null;
            }
            if (watchSubscription != null) {
                try {
                    watchSubscription.unsubscribe();
                } catch (Exception e) {
                    // Ignore
                }
                watchSubscription = null;
            }
        }

        private void stopAndDeleteMemberConsumer() {
            stopConsuming();
            try {
                JetStreamManagement jsm = nc.jetStreamManagement();
                String consumerName = composeStaticConsumerName(consumerGroupName, memberName);
                jsm.deleteConsumer(streamName, consumerName);
            } catch (Exception e) {
                // Ignore - consumer may not exist
            }
        }
    }
}
