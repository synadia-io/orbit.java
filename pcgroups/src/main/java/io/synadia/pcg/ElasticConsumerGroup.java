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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static io.synadia.pcg.PartitionUtils.*;

/**
 * Elastic consumer group implementation.
 * Provides dynamic partition assignment for consumer groups with a work queue stream.
 */
public class ElasticConsumerGroup {

    private static final Logger LOGGER = Logger.getLogger(ElasticConsumerGroup.class.getName());
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private ElasticConsumerGroup() {
        // Utility class
    }

    /**
     * Creates an elastic consumer group.
     *
     * @param nc                    NATS connection
     * @param streamName            Name of the source stream
     * @param consumerGroupName     Name of the consumer group
     * @param maxMembers            Maximum number of members (partitions)
     * @param filter                Subject filter with wildcards
     * @param partitioningWildcards Indexes of wildcards to use for partitioning
     * @param maxBufferedMsgs       Max messages in work queue (0 for unlimited)
     * @param maxBufferedBytes      Max bytes in work queue (0 for unlimited)
     * @return The created configuration
     */
    public static ElasticConsumerGroupConfig create(Connection nc, String streamName, String consumerGroupName,
                                                    int maxMembers, String filter, int[] partitioningWildcards,
                                                    long maxBufferedMsgs, long maxBufferedBytes)
            throws ConsumerGroupException, IOException, JetStreamApiException, InterruptedException {

        ElasticConsumerGroupConfig config = new ElasticConsumerGroupConfig(
                maxMembers, filter, partitioningWildcards, maxBufferedMsgs, maxBufferedBytes,
                new ArrayList<>(), new ArrayList<>());
        config.validate();

        JetStreamManagement jsm = nc.jetStreamManagement();

        // Get stream info to determine replicas and storage type
        StreamInfo streamInfo = jsm.getStreamInfo(streamName);
        int replicas = streamInfo.getConfiguration().getReplicas();
        StorageType storage = streamInfo.getConfiguration().getStorageType();

        // Get or create the KV bucket
        KeyValueManagement kvm = nc.keyValueManagement();
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            // Create the bucket if it doesn't exist
            KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                    .name(KV_ELASTIC_BUCKET_NAME)
                    .replicas(replicas)
                    .storageType(StorageType.File)
                    .build();
            kvm.create(kvConfig);
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        }

        String key = composeKey(streamName, consumerGroupName);

        // Check if config already exists
        KeyValueEntry entry = kv.get(key);
        if (entry != null) {
            String json = new String(entry.getValue(), StandardCharsets.UTF_8);
            ElasticConsumerGroupConfig existingConfig = GSON.fromJson(json, ElasticConsumerGroupConfig.class);

            // Verify the config matches
            if (existingConfig.getMaxMembers() != maxMembers ||
                    !Objects.equals(existingConfig.getFilter(), filter) ||
                    existingConfig.getMaxBufferedMsgs() != maxBufferedMsgs ||
                    existingConfig.getMaxBufferedBytes() != maxBufferedBytes ||
                    !Arrays.equals(existingConfig.getPartitioningWildcards(), partitioningWildcards)) {
                throw new ConsumerGroupException(
                        "the existing elastic consumer group config can not be updated to the requested one, " +
                                "please delete the existing elastic consumer group and create a new one");
            }
            return existingConfig;
        }

        // Create the config entry
        String payload = GSON.toJson(config);
        kv.put(key, payload.getBytes(StandardCharsets.UTF_8));

        // Create the work queue stream with subject transform
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
        String filterDest = getPartitioningTransformDest(config);

        StreamConfiguration.Builder scBuilder = StreamConfiguration.builder()
                .name(workQueueStreamName)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .replicas(replicas)
                .storageType(storage)
                .discardPolicy(DiscardPolicy.New)
                .allowDirect(true);

        if (maxBufferedMsgs > 0) {
            scBuilder.maxMessages(maxBufferedMsgs);
        }
        if (maxBufferedBytes > 0) {
            scBuilder.maxBytes(maxBufferedBytes);
        }

        // Add source with subject transform
        External external = null; // Local stream
        scBuilder.addSource(Source.builder()
                .sourceName(streamName)
                .startSeq(0)
                .subjectTransforms(SubjectTransform.builder()
                        .source(filter)
                        .destination(filterDest)
                        .build())
                .build());

        try {
            jsm.addStream(scBuilder.build());
        } catch (JetStreamApiException e) {
            throw new ConsumerGroupException("can't create the elastic consumer group's stream: " + e.getMessage(), e);
        }

        return config;
    }

    /**
     * Starts consuming messages from an elastic consumer group.
     *
     * @param nc                NATS connection
     * @param streamName        Name of the source stream
     * @param consumerGroupName Name of the consumer group
     * @param memberName        Name of this member
     * @param handler           Message handler callback
     * @param consumerConfig    Consumer configuration (null for defaults)
     * @return A consume context for controlling the consumption lifecycle
     */
    public static ConsumerGroupConsumeContext consume(Connection nc, String streamName, String consumerGroupName,
                                                      String memberName, Consumer<ConsumerGroupMsg> handler,
                                                      ConsumerConfiguration consumerConfig)
            throws ConsumerGroupException, IOException, JetStreamApiException, InterruptedException {
        if (handler == null) {
            throw new ConsumerGroupException("a message handler must be provided");
        }

        if (consumerConfig == null) {
            consumerConfig = ConsumerConfiguration.builder()
                    .ackWait(DEFAULT_ACK_WAIT)
                    .ackPolicy(AckPolicy.Explicit)
                    .build();
        }

        if (consumerConfig.getAckPolicy() != null && consumerConfig.getAckPolicy() != AckPolicy.Explicit) {
            throw new ConsumerGroupException("the ack policy when consuming from elastic consumer groups must be explicit");
        }

        if (consumerConfig.getAckWait() == null || consumerConfig.getAckWait().isZero() || consumerConfig.getAckWait().isNegative()) {
            consumerConfig = ConsumerConfiguration.builder(consumerConfig).ackWait(DEFAULT_ACK_WAIT).build();
        }

        consumerConfig = ConsumerConfiguration.builder(consumerConfig)
                .inactiveThreshold(Duration.ofMillis(consumerConfig.getAckWait().toMillis() * CONSUMER_IDLE_TIMEOUT_FACTOR))
                .build();

        // Verify the work queue stream exists
        JetStreamManagement jsm = nc.jetStreamManagement();
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
        try {
            jsm.getStreamInfo(workQueueStreamName);
        } catch (JetStreamApiException e) {
            throw new ConsumerGroupException("the elastic consumer group's stream does not exist", e);
        }

        // Get the KV bucket
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        // Get the config
        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        return new ElasticConsumeContextImpl(nc, kv, streamName, consumerGroupName, memberName, config, handler, consumerConfig);
    }

    /**
     * Deletes an elastic consumer group.
     */
    public static void delete(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        JetStreamManagement jsm = nc.jetStreamManagement();

        // Get the KV bucket and delete the config entry
        try {
            KeyValue kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
            String key = composeKey(streamName, consumerGroupName);
            kv.delete(key);
        } catch (Exception e) {
            // Ignore if bucket or key doesn't exist
        }

        // Delete the work queue stream
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
        try {
            jsm.deleteStream(workQueueStreamName);
        } catch (JetStreamApiException e) {
            throw new ConsumerGroupException("could not delete the elastic consumer group's stream: " + e.getMessage(), e);
        }
    }

    /**
     * Lists elastic consumer groups for a stream.
     */
    public static List<String> list(Connection nc, String streamName)
            throws IOException, JetStreamApiException, InterruptedException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
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
     * Adds members to an elastic consumer group.
     *
     * @return The updated list of members
     */
    public static List<String> addMembers(Connection nc, String streamName, String consumerGroupName,
                                          List<String> memberNamesToAdd)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() ||
                consumerGroupName == null || consumerGroupName.isEmpty() ||
                memberNamesToAdd == null || memberNamesToAdd.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or elastic consumer group name or no member names");
        }

        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        if (!config.getMemberMappings().isEmpty()) {
            throw new ConsumerGroupException("can't add members to an elastic consumer group that uses member mappings");
        }

        Set<String> existingMembers = new LinkedHashSet<>(config.getMembers());
        for (String memberName : memberNamesToAdd) {
            if (memberName != null && !memberName.isEmpty()) {
                existingMembers.add(memberName);
            }
        }

        List<String> newMembers = new ArrayList<>(existingMembers);
        config.setMembers(newMembers);

        String payload = GSON.toJson(config);
        String key = composeKey(streamName, consumerGroupName);
        kv.update(key, payload.getBytes(StandardCharsets.UTF_8), config.getRevision());

        return newMembers;
    }

    /**
     * Removes members from an elastic consumer group.
     *
     * @return The updated list of members
     */
    public static List<String> deleteMembers(Connection nc, String streamName, String consumerGroupName,
                                             List<String> memberNamesToDrop)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() ||
                consumerGroupName == null || consumerGroupName.isEmpty() ||
                memberNamesToDrop == null || memberNamesToDrop.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or elastic consumer group name or no member names");
        }

        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        if (!config.getMemberMappings().isEmpty()) {
            throw new ConsumerGroupException("can't drop members from an elastic consumer group that uses member mappings");
        }

        Set<String> droppingMembers = new HashSet<>(memberNamesToDrop);
        List<String> newMembers = new ArrayList<>();

        for (String existingMember : config.getMembers()) {
            if (!droppingMembers.contains(existingMember)) {
                newMembers.add(existingMember);
            }
        }

        config.setMembers(newMembers);

        String payload = GSON.toJson(config);
        String key = composeKey(streamName, consumerGroupName);
        kv.update(key, payload.getBytes(StandardCharsets.UTF_8), config.getRevision());

        return newMembers;
    }

    /**
     * Sets member mappings for an elastic consumer group.
     */
    public static void setMemberMappings(Connection nc, String streamName, String consumerGroupName,
                                         List<MemberMapping> memberMappings)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() ||
                consumerGroupName == null || consumerGroupName.isEmpty() ||
                memberMappings == null || memberMappings.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or elastic consumer group name or member mappings");
        }

        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        config.setMembers(new ArrayList<>());
        config.setMemberMappings(memberMappings);
        config.validate();

        String payload = GSON.toJson(config);
        String key = composeKey(streamName, consumerGroupName);
        kv.put(key, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Deletes member mappings for an elastic consumer group.
     */
    public static void deleteMemberMappings(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() ||
                consumerGroupName == null || consumerGroupName.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or elastic consumer group name");
        }

        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        config.setMemberMappings(new ArrayList<>());

        String payload = GSON.toJson(config);
        String key = composeKey(streamName, consumerGroupName);
        kv.put(key, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Lists active members of an elastic consumer group.
     */
    public static List<String> listActiveMembers(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        if (config.getMembers().isEmpty() && config.getMemberMappings().isEmpty()) {
            return new ArrayList<>();
        }

        JetStreamManagement jsm = nc.jetStreamManagement();
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);

        List<String> activeMembers = new ArrayList<>();
        List<ConsumerInfo> consumers = jsm.getConsumers(workQueueStreamName);

        List<String> memberList = config.getMembers();
        List<MemberMapping> mappings = config.getMemberMappings();

        for (ConsumerInfo cInfo : consumers) {
            if (!memberList.isEmpty()) {
                for (String m : memberList) {
                    if (cInfo.getName().equals(m)) {
                        activeMembers.add(m);
                        break;
                    }
                }
            } else if (!mappings.isEmpty()) {
                for (MemberMapping mapping : mappings) {
                    if (cInfo.getName().equals(mapping.getMember())) {
                        activeMembers.add(mapping.getMember());
                        break;
                    }
                }
            }
        }

        return activeMembers;
    }

    /**
     * Checks if a member is included in the elastic consumer group and is active.
     *
     * @return A boolean array where [0] = inMembership, [1] = isActive
     */
    public static boolean[] isInMembershipAndActive(Connection nc, String streamName, String consumerGroupName,
                                                    String memberName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        ElasticConsumerGroupConfig config = getConfigFromKV(kv, streamName, consumerGroupName);

        boolean inMembership = config.isInMembership(memberName);

        JetStreamManagement jsm = nc.jetStreamManagement();
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);

        boolean isActive = false;
        List<ConsumerInfo> consumers = jsm.getConsumers(workQueueStreamName);

        for (ConsumerInfo cInfo : consumers) {
            if (cInfo.getName().equals(memberName)) {
                isActive = true;
                break;
            }
        }

        return new boolean[]{inMembership, isActive};
    }

    /**
     * Forces the current active (pinned) instance for a member to step down.
     * This requires NATS server 2.11+ with priority consumer support.
     */
    public static void memberStepDown(Connection nc, String streamName, String consumerGroupName, String memberName)
            throws IOException, JetStreamApiException, InterruptedException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
        jsm.unpinConsumer(workQueueStreamName, memberName, PRIORITY_GROUP_NAME);
    }

    /**
     * Gets the elastic consumer group configuration.
     */
    public static ElasticConsumerGroupConfig getConfig(Connection nc, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        KeyValue kv;
        try {
            kv = nc.keyValue(KV_ELASTIC_BUCKET_NAME);
        } catch (Exception e) {
            throw new ConsumerGroupException("the elastic consumer group KV bucket doesn't exist", e);
        }

        return getConfigFromKV(kv, streamName, consumerGroupName);
    }

    /**
     * Returns the list of partition filters for a given member based on the config.
     */
    public static List<String> getPartitionFilters(ElasticConsumerGroupConfig config, String memberName) {
        return PartitionUtils.generatePartitionFilters(
                config.getMembers(), config.getMaxMembers(), config.getMemberMappings(), memberName);
    }

    private static ElasticConsumerGroupConfig getConfigFromKV(KeyValue kv, String streamName, String consumerGroupName)
            throws IOException, JetStreamApiException, InterruptedException, ConsumerGroupException {
        if (streamName == null || streamName.isEmpty() ||
                consumerGroupName == null || consumerGroupName.isEmpty()) {
            throw new ConsumerGroupException("invalid stream name or elastic consumer group name");
        }

        String key = composeKey(streamName, consumerGroupName);
        KeyValueEntry entry = kv.get(key);

        if (entry == null) {
            throw new ConsumerGroupException("error getting the elastic consumer group's config: not found");
        }

        String json = new String(entry.getValue(), StandardCharsets.UTF_8);
        ElasticConsumerGroupConfig config = GSON.fromJson(json, ElasticConsumerGroupConfig.class);
        config.setRevision(entry.getRevision());
        config.validate();

        return config;
    }

    private static String getPartitioningTransformDest(ElasticConsumerGroupConfig config) {
        int[] wildcards = config.getPartitioningWildcards();
        StringBuilder wildcardList = new StringBuilder();
        for (int i = 0; i < wildcards.length; i++) {
            if (i > 0) wildcardList.append(",");
            wildcardList.append(wildcards[i]);
        }

        String[] filterTokens = config.getFilter().split("\\.");
        int cwIndex = 1;
        for (int i = 0; i < filterTokens.length; i++) {
            if (filterTokens[i].equals("*")) {
                filterTokens[i] = "{{Wildcard(" + cwIndex + ")}}";
                cwIndex++;
            }
        }

        String destFromFilter = String.join(".", filterTokens);
        return "{{Partition(" + config.getMaxMembers() + "," + wildcardList + ")}}." + destFromFilter;
    }

    /**
     * Composes the Consumer Group Stream Name.
     */
    private static String composeCGSName(String streamName, String consumerGroupName) {
        return streamName + "-" + consumerGroupName;
    }

    /**
     * Internal implementation of the consume context for elastic consumer groups.
     * Uses a single event-processing thread (matching Go's instanceRoutine pattern)
     * to serialize watcher updates and self-correction, avoiding race conditions.
     */
    private static class ElasticConsumeContextImpl implements ConsumerGroupConsumeContext {
        // NATS API error code for "filtered consumer not unique on workqueue stream"
        private static final int JS_CONSUMER_WQ_CONSUMER_NOT_UNIQUE_ERR = 10100;

        private final Connection nc;
        private final KeyValue kv;
        private final String streamName;
        private final String consumerGroupName;
        private final String memberName;
        private ElasticConsumerGroupConfig config;
        private final Consumer<ConsumerGroupMsg> handler;
        private final ConsumerConfiguration consumerUserConfig;
        private final CompletableFuture<Void> doneFuture;
        private final AtomicBoolean stopped;
        private final AtomicReference<String> currentPinnedId;
        private final long selfCorrectionIntervalMs;

        // Event queue for serializing watcher updates (like Go's keyWatcher.Updates() channel)
        private final LinkedBlockingQueue<KeyValueEntry> eventQueue;

        // Single thread that processes all events (like Go's instanceRoutine goroutine)
        private Thread instanceThread;

        // Consumer state - only accessed from instanceThread (no synchronization needed)
        private MessageConsumer messageConsumer;
        private io.nats.client.impl.NatsKeyValueWatchSubscription watchSubscription;

        ElasticConsumeContextImpl(Connection nc, KeyValue kv, String streamName, String consumerGroupName,
                                  String memberName, ElasticConsumerGroupConfig config,
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
            this.eventQueue = new LinkedBlockingQueue<>();
            this.selfCorrectionIntervalMs = consumerUserConfig.getAckWait().toMillis() * CONSUMER_IDLE_TIMEOUT_FACTOR + 500;

            // Join if already in membership (before starting the routine, matching Go)
            if (config.isInMembership(memberName)) {
                joinMemberConsumer();
            }

            startWatcher();
            startInstanceRoutine();
        }

        /**
         * Sets up the KV watcher that enqueues events to the event queue.
         * The watcher callback does NO processing - it just enqueues, keeping the
         * NATS dispatch thread unblocked.
         */
        private void startWatcher() {
            Thread watcherThread = new Thread(() -> {
                try {
                    String key = composeKey(streamName, consumerGroupName);
                    KeyValueWatcher watcher = new KeyValueWatcher() {
                        @Override
                        public void watch(KeyValueEntry entry) {
                            if (!stopped.get()) {
                                eventQueue.offer(entry);
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

        /**
         * Starts the single instance routine thread that processes all events.
         * This matches Go's instanceRoutine goroutine with its select loop:
         * - Watcher updates from the event queue
         * - Self-correction via queue poll timeout
         * - Stop via the stopped flag
         */
        private void startInstanceRoutine() {
            instanceThread = new Thread(() -> {
                while (!stopped.get()) {
                    try {
                        // Poll with timeout - timeout acts as self-correction timer
                        // (like Go's time.After case in the select loop)
                        KeyValueEntry entry = eventQueue.poll(selfCorrectionIntervalMs, TimeUnit.MILLISECONDS);

                        if (stopped.get()) {
                            break;
                        }

                        if (entry == null) {
                            // Timeout = self-correction (like Go's time.After case)
                            if (messageConsumer == null && config.isInMembership(memberName)) {
                                joinMemberConsumer();
                            }
                        } else {
                            // Watcher update (like Go's keyWatcher.Updates() case)
                            processWatcherUpdate(entry);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // Cleanup on exit
                stopConsuming();
                doneFuture.complete(null);
            });
            instanceThread.setDaemon(true);
            instanceThread.start();
        }

        /**
         * Processes a single watcher update event.
         * Matches the watcher update case in Go's instanceRoutine select loop.
         */
        private void processWatcherUpdate(KeyValueEntry entry) {
            if (entry.getOperation() == KeyValueOperation.DELETE ||
                    entry.getOperation() == KeyValueOperation.PURGE) {
                stopConsuming();
                stopped.set(true);
                doneFuture.complete(null);
                return;
            }

            try {
                String json = new String(entry.getValue(), StandardCharsets.UTF_8);
                ElasticConsumerGroupConfig newConfig = GSON.fromJson(json, ElasticConsumerGroupConfig.class);
                newConfig.validate();

                // Check if critical config changed (immutable fields)
                if (newConfig.getMaxMembers() != config.getMaxMembers() ||
                        !Objects.equals(newConfig.getFilter(), config.getFilter()) ||
                        newConfig.getMaxBufferedMsgs() != config.getMaxBufferedMsgs() ||
                        newConfig.getMaxBufferedBytes() != config.getMaxBufferedBytes() ||
                        !Arrays.equals(newConfig.getPartitioningWildcards(), config.getPartitioningWildcards())) {
                    stopConsuming();
                    stopped.set(true);
                    doneFuture.completeExceptionally(
                            new ConsumerGroupException("elastic consumer group config watcher received a bad change in the configuration"));
                    return;
                }

                // Optimization: if nothing changed and already have the consumer, skip
                // (matches Go's optimization at instanceRoutine line 203-206)
                if (messageConsumer != null &&
                        Objects.equals(newConfig.getMembers(), config.getMembers()) &&
                        Objects.equals(newConfig.getMemberMappings(), config.getMemberMappings())) {
                    return;
                }

                // Check if members or mappings changed
                if (!Objects.equals(newConfig.getMembers(), config.getMembers()) ||
                        !Objects.equals(newConfig.getMemberMappings(), config.getMemberMappings())) {
                    config = newConfig;
                    processMembershipChange();
                }

            } catch (ConsumerGroupException e) {
                stopConsuming();
                stopped.set(true);
                doneFuture.completeExceptionally(e);
            } catch (Exception e) {
                LOGGER.warning("Error processing watcher update: " + e.getMessage());
            }
        }

        /**
         * Processes membership changes. Matches Go's processMembershipChange.
         * Only called from instanceThread - no synchronization needed.
         */
        private void processMembershipChange() {
            // Check if we are the pinned member before stopping
            boolean isPinned = false;
            if (messageConsumer != null) {
                try {
                    JetStreamManagement jsm = nc.jetStreamManagement();
                    String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
                    ConsumerInfo ci = jsm.getConsumerInfo(workQueueStreamName, memberName);
                    List<PriorityGroupState> pgStates = ci.getPriorityGroupStates();
                    if (pgStates != null) {
                        String myPinnedId = currentPinnedId.get();
                        for (PriorityGroupState pg : pgStates) {
                            if (PRIORITY_GROUP_NAME.equals(pg.getGroup()) &&
                                    myPinnedId != null && myPinnedId.equals(pg.getPinnedClientId())) {
                                isPinned = true;
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    // Ignore - consumer may not exist yet
                }
                // Stop the message consumer (matching Go's stopConsuming)
                stopMessageConsumer();
            }

            // Only the pinned member should delete the consumer
            if (isPinned) {
                try {
                    JetStreamManagement jsm = nc.jetStreamManagement();
                    String workQueueStreamName = composeCGSName(streamName, consumerGroupName);
                    jsm.deleteConsumer(workQueueStreamName, memberName);
                } catch (Exception e) {
                    // Ignore - consumer may not exist
                }
            } else {
                // Backoff to let the pinned member handle the delete and recreate first
                try {
                    Thread.sleep(400 + (long) (Math.random() * 100));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Rejoin (matching Go which always calls joinMemberConsumer unconditionally)
            joinMemberConsumer();
        }

        /**
         * Creates or recreates the JetStream consumer and starts consuming.
         * Matches Go's joinMemberConsumer. Only called from instanceThread.
         */
        private void joinMemberConsumer() {
            try {
                List<String> filters = ElasticConsumerGroup.getPartitionFilters(config, memberName);

                // If we are no longer in the membership list, nothing to do
                if (filters.isEmpty()) {
                    return;
                }

                JetStreamManagement jsm = nc.jetStreamManagement();
                String workQueueStreamName = composeCGSName(streamName, consumerGroupName);

                // Build consumer configuration from user config, overriding internal fields
                Duration pinnedTTL = calculatePinnedTTL(consumerUserConfig.getAckWait());
                ConsumerConfiguration cc = ConsumerConfiguration.builder(consumerUserConfig)
                        .durable(null)
                        .name(memberName)
                        .filterSubjects(filters)
                        .priorityGroups(PRIORITY_GROUP_NAME)
                        .priorityPolicy(PriorityPolicy.PinnedClient)
                        .priorityTimeout(pinnedTTL)
                        .build();

                // Try to create consumer (matching Go's tryCreateConsumer pattern)
                try {
                    jsm.createConsumer(workQueueStreamName, cc);
                } catch (JetStreamApiException e) {
                    // Check for "filtered consumer not unique on workqueue" - silently ignore
                    // (normal during concurrent membership changes, self-correction will retry)
                    if (e.getApiErrorCode() == JS_CONSUMER_WQ_CONSUMER_NOT_UNIQUE_ERR) {
                        return;
                    }
                    // Consumer exists with different config - delete and recreate
                    // (matching Go's ErrConsumerExists handling)
                    try {
                        jsm.deleteConsumer(workQueueStreamName, memberName);
                    } catch (Exception deleteEx) {
                        LOGGER.warning("Error trying to delete consumer for member \"" + memberName + "\": " + deleteEx.getMessage());
                        return;
                    }
                    try {
                        jsm.createConsumer(workQueueStreamName, cc);
                    } catch (JetStreamApiException retryEx) {
                        if (retryEx.getApiErrorCode() == JS_CONSUMER_WQ_CONSUMER_NOT_UNIQUE_ERR) {
                            return;
                        }
                        LOGGER.warning("Error trying to create consumer for member \"" + memberName + "\": " + retryEx.getMessage());
                        return;
                    } catch (Exception retryEx) {
                        LOGGER.warning("Error trying to create consumer for member \"" + memberName + "\": " + retryEx.getMessage());
                        return;
                    }
                }

                // Start consuming (matching Go's startConsuming)
                StreamContext sc = nc.getStreamContext(workQueueStreamName);
                ConsumerContext consumerCtx = sc.getConsumerContext(memberName);

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

            } catch (Exception e) {
                LOGGER.warning("Error joining member consumer: " + e.getMessage());
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                // Interrupt the instance thread to wake it from queue.poll()
                if (instanceThread != null) {
                    instanceThread.interrupt();
                }
            }
        }

        @Override
        public CompletableFuture<Void> done() {
            return doneFuture;
        }

        /**
         * Stops the JetStream message consumer by closing it (unsubscribing the pull subscription).
         * Uses close() instead of stop() because:
         * - stop() only sets a flag but leaves the subscription active
         * - close() sets the flag AND unsubscribes the subscription from the dispatcher
         * This matches Go's consumeContext.Stop() which cancels the consume loop and waits
         * for exit, ensuring no pending pull requests remain when the consumer is deleted.
         */
        private void stopMessageConsumer() {
            if (messageConsumer != null) {
                try {
                    messageConsumer.close();
                } catch (Exception e) {
                    // Ignore
                }
                messageConsumer = null;
            }
        }

        /**
         * Stops everything: message consumer and KV watcher.
         * Used for full shutdown only (called from instanceThread on exit).
         * Matches Go's stopConsuming() + watcher cleanup.
         */
        private void stopConsuming() {
            stopMessageConsumer();
            if (watchSubscription != null) {
                try {
                    watchSubscription.unsubscribe();
                } catch (Exception e) {
                    // Ignore
                }
                watchSubscription = null;
            }
        }
    }
}
