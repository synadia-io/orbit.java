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
import static io.nats.client.support.JsonValueUtils.readValue;

/**
 * Configuration for a static consumer group.
 * JSON structure must be compatible with the Go version.
 */
public class StaticConsumerGroupConfig implements JsonSerializable {
    static final String MAX_MEMBERS = "max_members";
    static final String FILTER = "filter";
    static final String MEMBERS = "members";
    static final String MEMBER_MAPPINGS = "member_mappings";

    private int maxMembers;
    private String filter;
    private List<String> members;
    private List<MemberMapping> memberMappings;

    @Nullable
    public static StaticConsumerGroupConfig instance(KeyValueEntry entry) throws JsonParseException {
        if (entry != null) {
            byte[] json = entry.getValue();
            if (json != null) {
                return instance(json);
            }
        }
        return null;
    }

    @NonNull
    public static StaticConsumerGroupConfig instance(byte @NonNull[] json) throws JsonParseException {
        return new StaticConsumerGroupConfig(JsonParser.parse(json));
    }

    public StaticConsumerGroupConfig() {
        this.members = new ArrayList<>();
        this.memberMappings = new ArrayList<>();
    }

    public StaticConsumerGroupConfig(int maxMembers, String filter, List<String> members, List<MemberMapping> memberMappings) {
        this.maxMembers = maxMembers;
        this.filter = filter;
        this.members = members == null ? new ArrayList<>() : new ArrayList<>(members);
        this.memberMappings = memberMappings == null ? new ArrayList<>() : new ArrayList<>(memberMappings);
    }

    public StaticConsumerGroupConfig(JsonValue jv) {
        this.maxMembers = JsonValueUtils.readInteger(jv, MAX_MEMBERS, 0);
        this.filter = JsonValueUtils.readString(jv, FILTER);
        this.members = JsonValueUtils.readStringList(jv, MEMBERS);
        this.memberMappings = MemberMapping.listOfOrEmptyList(readValue(jv, MEMBER_MAPPINGS));
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, MAX_MEMBERS, maxMembers);
        addField(sb, FILTER, filter);
        addStrings(sb, MEMBERS, members);
        addJsons(sb, MEMBER_MAPPINGS, memberMappings);
        return endJson(sb).toString();
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

    public List<String> getMembers() {
        return new ArrayList<>(members);
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
     * Validates the static consumer group configuration.
     *
     * @throws ConsumerGroupException if the configuration is invalid
     */
    public void validate() throws ConsumerGroupException {
        // Validate max members
        if (maxMembers < 1) {
            throw new ConsumerGroupException("the max number of members must be >= 1");
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StaticConsumerGroupConfig that = (StaticConsumerGroupConfig) o;
        return maxMembers == that.maxMembers &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(members, that.members) &&
                Objects.equals(memberMappings, that.memberMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxMembers, filter, members, memberMappings);
    }

    @Override
    public String toString() {
        return "StaticConsumerGroupConfig{" +
                "maxMembers=" + maxMembers +
                ", filter='" + filter + '\'' +
                ", members=" + members +
                ", memberMappings=" + memberMappings +
                '}';
    }
}
