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

import io.nats.client.api.KeyValueEntry;
import io.nats.client.support.*;
import io.synadia.pcg.exceptions.ConsumerGroupException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.*;

import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.*;

/**
 * Configuration for an elastic consumer group.
 * JSON structure must be compatible with the Go version.
 */
public class ElasticConsumerGroupConfig implements JsonSerializable {
    static final String MAX_MEMBERS = "max_members";
    static final String FILTER = "filter";
    static final String PARTITIONING_WILDCARDS = "partitioning_wildcards";
    static final String MAX_BUFFERED_MSG = "max_buffered_msg";
    static final String MAX_BUFFERED_BYTES = "max_buffered_bytes";
    static final String MEMBERS = "members";
    static final String MEMBER_MAPPINGS = "member_mappings";

    private int maxMembers;
    private String filter;
    private int[] partitioningWildcards;
    private long maxBufferedMessages;
    private long maxBufferedBytes;
    private List<String> members;
    private List<MemberMapping> memberMappings;

    // Internal revision number, not serialized
    private transient long revision;

    @Nullable
    public static ElasticConsumerGroupConfig instance(KeyValueEntry entry) throws JsonParseException {
        if (entry != null) {
            byte[] json = entry.getValue();
            if (json != null) {
                ElasticConsumerGroupConfig config = instance(json);
                config.setRevision(entry.getRevision());
                return config;
            }
        }
        return null;
    }

    @NonNull
    public static ElasticConsumerGroupConfig instance(byte @NonNull[] json) throws JsonParseException {
        return new ElasticConsumerGroupConfig(JsonParser.parse(json));
    }

    public ElasticConsumerGroupConfig() {
        this.partitioningWildcards = new int[0];
        this.members = new ArrayList<>();
        this.memberMappings = new ArrayList<>();
    }

    public ElasticConsumerGroupConfig(int maxMembers, String filter, int[] partitioningWildcards,
                                      long maxBufferedMessages, long maxBufferedBytes,
                                      List<String> members, List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.filter = filter;
        this.partitioningWildcards = partitioningWildcards != null ? partitioningWildcards.clone() : new int[0];
        this.maxBufferedMessages = maxBufferedMessages;
        this.maxBufferedBytes = maxBufferedBytes;
        this.members = members == null ? new ArrayList<>() : new ArrayList<>(members);
        this.memberMappings = memberMappings == null ? new ArrayList<>() : new ArrayList<>(memberMappings);
    }

    public ElasticConsumerGroupConfig(JsonValue jv) {
        this.maxMembers = JsonValueUtils.readInteger(jv, MAX_MEMBERS, 0);
        this.filter = JsonValueUtils.readString(jv, FILTER);
        List<Integer> integers = read(jv, PARTITIONING_WILDCARDS, v -> listOf(v, JsonValueUtils::getInteger));
        this.partitioningWildcards = new int[integers.size()];
        for (int x = 0; x < integers.size(); x++) {
            Integer i = integers.get(x);
            this.partitioningWildcards[x] = i == null ? 0 : i;
        }
        this.maxBufferedMessages = JsonValueUtils.readLong(jv, MAX_BUFFERED_MSG, 0);
        this.maxBufferedBytes = JsonValueUtils.readLong(jv, MAX_BUFFERED_BYTES, 0);
        this.members = JsonValueUtils.readStringList(jv, MEMBERS);
        this.memberMappings = MemberMapping.listOfOrEmptyList(readValue(jv, MEMBER_MAPPINGS));
    }

    public int getMaxMembers() {
        return maxMembers;
    }

    public void setMaxMembers(int maxMembers) {
        this.maxMembers = maxMembers;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public int[] getPartitioningWildcards() {
        return partitioningWildcards.clone();
    }

    public void setPartitioningWildcards(int[] partitioningWildcards) {
        this.partitioningWildcards = partitioningWildcards == null ? new int[0] : partitioningWildcards.clone();
    }

    public long getMaxBufferedMessages() {
        return maxBufferedMessages;
    }

    public void setMaxBufferedMessages(long maxBufferedMessages) {
        this.maxBufferedMessages = maxBufferedMessages;
    }

    public long getMaxBufferedBytes() {
        return maxBufferedBytes;
    }

    public void setMaxBufferedBytes(long maxBufferedBytes) {
        this.maxBufferedBytes = maxBufferedBytes;
    }

    public List<String> getMembers() {
        return members != null ? new ArrayList<>(members) : new ArrayList<>();
    }

    public void setMembers(List<String> members) {
        this.members = members == null ? new ArrayList<>() : new ArrayList<>(members);
    }

    public List<MemberMapping> getMemberMappings() {
        return new ArrayList<>(memberMappings);
    }

    public void setMemberMappings(List<MemberMapping> memberMappings) {
        this.memberMappings = memberMappings == null ? new ArrayList<>() : new ArrayList<>(memberMappings);
    }

    public long getRevision() {
        return revision;
    }

    public void setRevision(long revision) {
        this.revision = revision;
    }

    /**
     * Checks if the given member name is in the current membership.
     */
    public boolean isInMembership(String name) {
        if (!memberMappings.isEmpty()) {
            for (MemberMapping mapping : memberMappings) {
                if (mapping.getMember().equals(name)) {
                    return true;
                }
            }
        }
        return members.contains(name);
    }

    /**
     * Validates the elastic consumer group configuration.
     *
     * @throws ConsumerGroupException if the configuration is invalid
     */
    public void validate() throws ConsumerGroupException {
        // Validate max members
        if (maxMembers < 1) {
            throw new ConsumerGroupException("the max number of members must be >= 1");
        }

        // Validate filter and partitioning wildcards
        if (filter == null || filter.isEmpty()) {
            throw new ConsumerGroupException("filter must not be empty");
        }

        String[] filterTokens = filter.split("\\.");
        int numWildcards = 0;
        for (String token : filterTokens) {
            if ("*".equals(token)) {
                numWildcards++;
            }
        }

        if (numWildcards < 1) {
            throw new ConsumerGroupException("filter must contain at least one * wildcard");
        }

        if (partitioningWildcards == null || partitioningWildcards.length < 1 || partitioningWildcards.length > numWildcards) {
            throw new ConsumerGroupException("the number of partitioning wildcards must be between 1 and the total number of * wildcards in the filter");
        }

        Set<Integer> seenWildcards = new HashSet<>();
        for (int pwc : partitioningWildcards) {
            if (seenWildcards.contains(pwc)) {
                throw new ConsumerGroupException("partitioning wildcard indexes must be unique");
            }
            seenWildcards.add(pwc);

            if (pwc < 1 || pwc > numWildcards) {
                throw new ConsumerGroupException("partitioning wildcard indexes must be greater than 1 and less than or equal to the number of * wildcards in the filter");
            }
        }

        // Validate that only one of members or member mappings is provided
        boolean hasMembers = !members.isEmpty();
        boolean hasMemberMappings = !memberMappings.isEmpty();

        if (hasMembers && hasMemberMappings) {
            throw new ConsumerGroupException("either members or member mappings must be provided, not both");
        }

        // Validate member mappings
        if (hasMemberMappings) {
            if (memberMappings.size() > maxMembers) {
                throw new ConsumerGroupException("the number of member mappings must be between 1 and the max number of members");
            }

            Set<String> seenMembers = new HashSet<>();
            Set<Integer> seenPartitions = new HashSet<>();

            for (MemberMapping mm : memberMappings) {
                if (seenMembers.contains(mm.getMember())) {
                    throw new ConsumerGroupException("member names must be unique");
                }
                seenMembers.add(mm.getMember());

                for (int p : mm.getPartitions()) {
                    if (seenPartitions.contains(p)) {
                        throw new ConsumerGroupException("partition numbers must be used only once");
                    }
                    seenPartitions.add(p);

                    if (p < 0 || p >= maxMembers) {
                        throw new ConsumerGroupException("partition numbers must be between 0 and one less than the max number of members");
                    }
                }
            }

            if (seenPartitions.size() != maxMembers) {
                throw new ConsumerGroupException("the number of unique partition numbers must be equal to the max number of members");
            }
        }
    }

    /**
     * Generates the subject transform destination for partitioning.
     */
    public String getPartitioningTransformDest() {
        StringBuilder wildcardList = new StringBuilder();
        for (int i = 0; i < partitioningWildcards.length; i++) {
            if (i > 0) {
                wildcardList.append(",");
            }
            wildcardList.append(partitioningWildcards[i]);
        }

        String[] filterTokens = filter.split("\\.");
        int cwIndex = 1;
        for (int i = 0; i < filterTokens.length; i++) {
            if ("*".equals(filterTokens[i])) {
                filterTokens[i] = "{{Wildcard(" + cwIndex + ")}}";
                cwIndex++;
            }
        }

        String destFromFilter = String.join(".", filterTokens);
        return "{{Partition(" + maxMembers + "," + wildcardList + ")}}." + destFromFilter;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, MAX_MEMBERS, maxMembers);
        addField(sb, FILTER, filter);
        if (partitioningWildcards != null && partitioningWildcards.length > 0) {
            sb.append("\"").append(PARTITIONING_WILDCARDS).append("\":[");
            for (int i = 0; i < partitioningWildcards.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(partitioningWildcards[i]);
            }
            sb.append("],");
        }
        addField(sb, MAX_BUFFERED_MSG, maxBufferedMessages);
        addField(sb, MAX_BUFFERED_BYTES, maxBufferedBytes);
        addStrings(sb, MEMBERS, members);
        addJsons(sb, MEMBER_MAPPINGS, memberMappings);
        return endJson(sb).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticConsumerGroupConfig that = (ElasticConsumerGroupConfig) o;
        return maxMembers == that.maxMembers &&
                maxBufferedMessages == that.maxBufferedMessages &&
                maxBufferedBytes == that.maxBufferedBytes &&
                Objects.equals(filter, that.filter) &&
                Arrays.equals(partitioningWildcards, that.partitioningWildcards) &&
                Objects.equals(members, that.members) &&
                Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(maxMembers, filter, maxBufferedMessages, maxBufferedBytes, members, memberMappings);
        result = 31 * result + Arrays.hashCode(partitioningWildcards);
        return result;
    }

    @Override
    public String toString() {
        return "ElasticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", filter='" + filter + '\'' +
                ", partitioningWildcards=" + Arrays.toString(partitioningWildcards) +
                ", maxBufferedMsgs=" + maxBufferedMessages +
                ", maxBufferedBytes=" + maxBufferedBytes +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}
