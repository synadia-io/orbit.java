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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * Represents a partitioning filter with its associated wildcard indexes.
 * JSON structure must be compatible with the Go version.
 */
public class PartitioningFilter implements JsonSerializable {
    static final String FILTER = "filter";
    static final String PARTITIONING_WILDCARDS = "partitioning_wildcards";

    public static PartitioningFilter EVERYTHING = new PartitioningFilter(">", new int[0]);

    private String filter;
    private int[] partitioningWildcards;

    static List<PartitioningFilter> listOfOrEmptyList(JsonValue jv) {
        return JsonValueUtils.listOf(jv, PartitioningFilter::new);
    }

    public PartitioningFilter() {
        this.partitioningWildcards = new int[0];
    }

    public PartitioningFilter(String filter) {
        this(filter, new int[0]);
    }

    public PartitioningFilter(String filter, int[] partitioningWildcards) {
        this.filter = filter;
        this.partitioningWildcards = partitioningWildcards != null ? partitioningWildcards.clone() : new int[0];
    }

    public PartitioningFilter(JsonValue jv) {
        this.filter = readString(jv, FILTER);
        List<Integer> integers = read(jv, PARTITIONING_WILDCARDS, v -> listOf(v, JsonValueUtils::getInteger));
        if (integers == null || integers.isEmpty()) {
            this.partitioningWildcards = new int[0];
        } else {
            this.partitioningWildcards = new int[integers.size()];
            for (int x = 0; x < integers.size(); x++) {
                Integer i = integers.get(x);
                this.partitioningWildcards[x] = i == null ? 0 : i;
            }
        }
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

    public String getPartitioningTransformDest(int maxMembers) {
        String effectiveFilter = (getFilter() != null && !getFilter().isEmpty()) ? getFilter() : ">";
        int[] wildcards = getPartitioningWildcards();

        StringBuilder wildcardList = new StringBuilder();
        for (int i = 0; i < wildcards.length; i++) {
            if (i > 0) wildcardList.append(",");
            wildcardList.append(wildcards[i]);
        }

        String[] filterTokens = effectiveFilter.split("\\.");
        int cwIndex = 1;
        for (int i = 0; i < filterTokens.length; i++) {
            if (filterTokens[i].equals("*")) {
                filterTokens[i] = "{{Wildcard(" + cwIndex + ")}}";
                cwIndex++;
            }
        }

        String destFromFilter = String.join(".", filterTokens);

        if (wildcards.length == 0) {
            return "{{Partition(" + maxMembers + ")}}." + destFromFilter;
        }

        return "{{Partition(" + maxMembers + "," + wildcardList + ")}}." + destFromFilter;
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, FILTER, filter);
        if (partitioningWildcards.length > 0) {
            List<Integer> integers = new ArrayList<>(partitioningWildcards.length);
            for (int i : partitioningWildcards) {
                integers.add(i);
            }
            _addList(sb, PARTITIONING_WILDCARDS, integers, StringBuilder::append);
        }
        return endJson(sb).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitioningFilter that = (PartitioningFilter) o;
        return Objects.equals(filter, that.filter) &&
                Arrays.equals(partitioningWildcards, that.partitioningWildcards);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(filter);
        result = 31 * result + Arrays.hashCode(partitioningWildcards);
        return result;
    }

    @Override
    public String toString() {
        return "PartitioningFilter{" +
                "filter='" + filter + '\'' +
                ", partitioningWildcards=" + Arrays.toString(partitioningWildcards) +
                '}';
    }
}
