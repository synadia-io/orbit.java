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

import com.google.gson.annotations.SerializedName;
import io.synadia.pcg.exceptions.ConsumerGroupException;

import java.util.*;

/**
 * Configuration for an elastic consumer group.
 * JSON structure must be compatible with the Go version.
 */
public class ElasticConsumerGroupConfig {

    @SerializedName("max_members")
    private int maxMembers;

    @SerializedName("filter")
    private String filter;

    @SerializedName("partitioning_wildcards")
    private int[] partitioningWildcards;

    @SerializedName("max_buffered_msg")
    private long maxBufferedMsgs;

    @SerializedName("max_buffered_bytes")
    private long maxBufferedBytes;

    @SerializedName("members")
    private List<String> members;

    @SerializedName("member_mappings")
    private List<MemberMapping> memberMappings;

    // Internal revision number, not serialized
    private transient long revision;

    public ElasticConsumerGroupConfig() {
        this.partitioningWildcards = new int[0];
        this.members = new ArrayList<>();
        this.memberMappings = new ArrayList<>();
    }

    public ElasticConsumerGroupConfig(int maxMembers, String filter, int[] partitioningWildcards,
                                      long maxBufferedMsgs, long maxBufferedBytes,
                                      List<String> members, List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.filter = filter;
        this.partitioningWildcards = partitioningWildcards != null ? partitioningWildcards.clone() : new int[0];
        this.maxBufferedMsgs = maxBufferedMsgs;
        this.maxBufferedBytes = maxBufferedBytes;
        this.members = members != null ? new ArrayList<>(members) : new ArrayList<>();
        this.memberMappings = memberMappings != null ? new ArrayList<>(memberMappings) : new ArrayList<>();
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
        return partitioningWildcards != null ? partitioningWildcards.clone() : new int[0];
    }

    public void setPartitioningWildcards(int[] partitioningWildcards) {
        this.partitioningWildcards = partitioningWildcards != null ? partitioningWildcards.clone() : new int[0];
    }

    public long getMaxBufferedMsgs() {
        return maxBufferedMsgs;
    }

    public void setMaxBufferedMsgs(long maxBufferedMsgs) {
        this.maxBufferedMsgs = maxBufferedMsgs;
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
        this.members = members != null ? new ArrayList<>(members) : new ArrayList<>();
    }

    public List<MemberMapping> getMemberMappings() {
        return memberMappings != null ? new ArrayList<>(memberMappings) : new ArrayList<>();
    }

    public void setMemberMappings(List<MemberMapping> memberMappings) {
        this.memberMappings = memberMappings != null ? new ArrayList<>(memberMappings) : new ArrayList<>();
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
        if (memberMappings != null && !memberMappings.isEmpty()) {
            for (MemberMapping mapping : memberMappings) {
                if (mapping.getMember().equals(name)) {
                    return true;
                }
            }
        }
        if (members != null && !members.isEmpty()) {
            return members.contains(name);
        }
        return false;
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
        boolean hasMembers = members != null && !members.isEmpty();
        boolean hasMemberMappings = memberMappings != null && !memberMappings.isEmpty();

        if (hasMembers && hasMemberMappings) {
            throw new ConsumerGroupException("either members or member mappings must be provided, not both");
        }

        // Validate member mappings
        if (hasMemberMappings) {
            if (memberMappings.size() < 1 || memberMappings.size() > maxMembers) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticConsumerGroupConfig that = (ElasticConsumerGroupConfig) o;
        return maxMembers == that.maxMembers &&
                maxBufferedMsgs == that.maxBufferedMsgs &&
                maxBufferedBytes == that.maxBufferedBytes &&
                Objects.equals(filter, that.filter) &&
                Arrays.equals(partitioningWildcards, that.partitioningWildcards) &&
                Objects.equals(members, that.members) &&
                Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(maxMembers, filter, maxBufferedMsgs, maxBufferedBytes, members, memberMappings);
        result = 31 * result + Arrays.hashCode(partitioningWildcards);
        return result;
    }

    @Override
    public String toString() {
        return "ElasticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", filter='" + filter + '\'' +
                ", partitioningWildcards=" + Arrays.toString(partitioningWildcards) +
                ", maxBufferedMsgs=" + maxBufferedMsgs +
                ", maxBufferedBytes=" + maxBufferedBytes +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}
