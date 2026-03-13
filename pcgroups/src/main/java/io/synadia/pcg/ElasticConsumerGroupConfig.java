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
    static final String PARTITIONING_FILTERS = "partitioning_filters";
    static final String MAX_BUFFERED_MSG = "max_buffered_msg";
    static final String MAX_BUFFERED_BYTES = "max_buffered_bytes";
    static final String MEMBERS = "members";
    static final String MEMBER_MAPPINGS = "member_mappings";

    private int maxMembers;
    private List<PartitioningFilter> partitioningFilters;
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
        this.partitioningFilters = new ArrayList<>();
        this.members = new ArrayList<>();
        this.memberMappings = new ArrayList<>();
    }

    public ElasticConsumerGroupConfig(int maxMembers, List<PartitioningFilter> partitioningFilters,
                                      long maxBufferedMessages, long maxBufferedBytes,
                                      List<String> members, List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.partitioningFilters = partitioningFilters == null ? new ArrayList<>() : new ArrayList<>(partitioningFilters);
        this.maxBufferedMessages = maxBufferedMessages;
        this.maxBufferedBytes = maxBufferedBytes;
        this.members = members == null ? new ArrayList<>() : new ArrayList<>(members);
        this.memberMappings = memberMappings == null ? new ArrayList<>() : new ArrayList<>(memberMappings);
    }

    public ElasticConsumerGroupConfig(JsonValue jv) {
        this.maxMembers = JsonValueUtils.readInteger(jv, MAX_MEMBERS, 0);
        this.partitioningFilters = PartitioningFilter.listOfOrEmptyList(readValue(jv, PARTITIONING_FILTERS));
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

    public List<PartitioningFilter> getPartitioningFilters() {
        return new ArrayList<>(partitioningFilters);
    }

    public void setPartitioningFilters(List<PartitioningFilter> partitioningFilters) {
        this.partitioningFilters = partitioningFilters == null ? new ArrayList<>() : new ArrayList<>(partitioningFilters);
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

        // Validate partitioning filters
        for (PartitioningFilter pf : partitioningFilters) {
            if (pf.getFilter() == null || pf.getFilter().isEmpty()) {
                throw new ConsumerGroupException("partitioning filters must have a non-empty filter");
            }

            String[] filterTokens = pf.getFilter().split("\\.");
            int numWildcards = 0;
            for (String token : filterTokens) {
                if ("*".equals(token)) {
                    numWildcards++;
                }
            }

            if (numWildcards == 0 && !">".equals(filterTokens[filterTokens.length - 1])) {
                throw new ConsumerGroupException("partitioning filters must have at least one * wildcard or end with > wildcard");
            }

            int[] wildcards = pf.getPartitioningWildcards();
            if (wildcards != null && wildcards.length > numWildcards) {
                throw new ConsumerGroupException("the number of partitioning wildcards must not be larger than the total number of * wildcards in the filter");
            }

            Set<Integer> seenWildcards = new HashSet<>();
            if (wildcards != null) {
                for (int pwc : wildcards) {
                    if (seenWildcards.contains(pwc)) {
                        throw new ConsumerGroupException("partitioning wildcard indexes must be unique");
                    }
                    seenWildcards.add(pwc);

                    if (pwc > numWildcards || pwc < 1) {
                        throw new ConsumerGroupException("partitioning wildcard indexes must be between 1 and the number of * wildcards in the filter");
                    }
                }
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

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, MAX_MEMBERS, maxMembers);
        addJsons(sb, PARTITIONING_FILTERS, partitioningFilters);
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
                Objects.equals(partitioningFilters, that.partitioningFilters) &&
                Objects.equals(members, that.members) &&
                Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMembers, partitioningFilters, maxBufferedMessages, maxBufferedBytes, members, memberMappings);
    }

    @Override
    public String toString() {
        return "ElasticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", partitioningFilters=" + partitioningFilters +
                ", maxBufferedMessages=" + maxBufferedMessages +
                ", maxBufferedBytes=" + maxBufferedBytes +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}
