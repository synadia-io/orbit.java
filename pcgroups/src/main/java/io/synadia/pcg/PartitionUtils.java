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

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility functions shared by both static and elastic consumer groups.
 * Contains constants and the partition distribution algorithm.
 */
public final class PartitionUtils {

    // Constants matching the Go implementation
    public static final int PULL_TIMEOUT_DIVIDER = 2;
    public static final int CONSUMER_IDLE_TIMEOUT_FACTOR = 1;
    public static final Duration DEFAULT_ACK_WAIT = Duration.ofSeconds(5);
    public static final Duration MIN_PULL_EXPIRY_PINNED_TTL = Duration.ofSeconds(1);
    public static final String PRIORITY_GROUP_NAME = "PCG";

    // KV bucket names (must match Go implementation)
    public static final String KV_STATIC_BUCKET_NAME = "static-consumer-groups";
    public static final String KV_ELASTIC_BUCKET_NAME = "elastic-consumer-groups";

    private PartitionUtils() {
        // Utility class
    }

    /**
     * Compose the consumer group's config key name.
     * Format: "{streamName}.{consumerGroupName}"
     */
    public static String composeKey(String streamName, String consumerGroupName) {
        return streamName + "." + consumerGroupName;
    }

    /**
     * Compose the consumer group stream name for elastic consumer groups.
     * Format: "{streamName}-{consumerGroupName}"
     */
    public static String composeCGSName(String streamName, String consumerGroupName) {
        return streamName + "-" + consumerGroupName;
    }

    /**
     * Compose the static consumer name.
     * Format: "{consumerGroupName}-{memberName}"
     */
    public static String composeStaticConsumerName(String consumerGroupName, String memberName) {
        return consumerGroupName + "-" + memberName;
    }

    /**
     * Generates the partition filters for a particular member of a consumer group,
     * according to the provided max number of members and the membership.
     * This algorithm must match the Go implementation exactly for wire compatibility.
     *
     * @param members        List of member names (for balanced distribution)
     * @param maxMembers     Maximum number of members/partitions
     * @param memberMappings Explicit member-to-partition mappings (alternative to members)
     * @param memberName     The member to generate filters for
     * @return List of partition filters (e.g., "0.>", "1.>", "2.>")
     */
    public static List<String> generatePartitionFilters(List<String> members, int maxMembers,
                                                        List<MemberMapping> memberMappings, String memberName) {
        if (members != null && !members.isEmpty()) {
            // Deduplicate and sort members
            List<String> sortedMembers = members.stream()
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());

            // Cap to maxMembers if necessary
            if (sortedMembers.size() > maxMembers) {
                sortedMembers = sortedMembers.subList(0, maxMembers);
            }

            int numMembers = sortedMembers.size();

            if (numMembers > 0) {
                // Rounded number of partitions per member
                int numPer = maxMembers / numMembers;
                List<String> myFilters = new ArrayList<>();

                for (int i = 0; i < maxMembers; i++) {
                    int memberIndex = i / numPer;

                    if (i < (numMembers * numPer)) {
                        if (sortedMembers.get(memberIndex % numMembers).equals(memberName)) {
                            myFilters.add(i + ".>");
                        }
                    } else {
                        // Remainder if the number of partitions is not a multiple of the number of members
                        if (sortedMembers.get((i - (numMembers * numPer)) % numMembers).equals(memberName)) {
                            myFilters.add(i + ".>");
                        }
                    }
                }

                return myFilters;
            }
            return Collections.emptyList();
        } else if (memberMappings != null && !memberMappings.isEmpty()) {
            List<String> myFilters = new ArrayList<>();

            for (MemberMapping mapping : memberMappings) {
                if (mapping.getMember().equals(memberName)) {
                    for (int pn : mapping.getPartitions()) {
                        myFilters.add(pn + ".>");
                    }
                }
            }

            return myFilters;
        }
        return Collections.emptyList();
    }

    /**
     * Deduplicate a list of strings while preserving order.
     */
    public static List<String> deduplicateStringList(List<String> list) {
        if (list == null) {
            return Collections.emptyList();
        }
        Set<String> seen = new LinkedHashSet<>();
        List<String> result = new ArrayList<>();
        for (String s : list) {
            if (seen.add(s)) {
                result.add(s);
            }
        }
        return result;
    }

    /**
     * Calculate the pull expiry duration based on ack wait.
     */
    public static Duration calculatePullExpiry(Duration ackWait) {
        Duration calculated = ackWait.dividedBy(PULL_TIMEOUT_DIVIDER);
        return calculated.compareTo(MIN_PULL_EXPIRY_PINNED_TTL) > 0 ? calculated : MIN_PULL_EXPIRY_PINNED_TTL;
    }

    /**
     * Calculate the pinned TTL based on ack wait.
     */
    public static Duration calculatePinnedTTL(Duration ackWait) {
        return ackWait.compareTo(MIN_PULL_EXPIRY_PINNED_TTL) > 0 ? ackWait : MIN_PULL_EXPIRY_PINNED_TTL;
    }

    /**
     * Calculate the inactive threshold based on ack wait.
     */
    public static Duration calculateInactiveThreshold(Duration ackWait) {
        return ackWait.multipliedBy(CONSUMER_IDLE_TIMEOUT_FACTOR);
    }
}
