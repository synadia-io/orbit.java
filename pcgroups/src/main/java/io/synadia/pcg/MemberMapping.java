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
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a mapping between a member name and its assigned partitions.
 * JSON structure must be compatible with the Go version.
 */
public class MemberMapping {

    @SerializedName("member")
    private String member;

    @SerializedName("partitions")
    private int[] partitions;

    public MemberMapping() {
    }

    public MemberMapping(String member, int[] partitions) {
        this.member = member;
        this.partitions = partitions != null ? partitions.clone() : new int[0];
    }

    public String getMember() {
        return member;
    }

    public void setMember(String member) {
        this.member = member;
    }

    public int[] getPartitions() {
        return partitions != null ? partitions.clone() : new int[0];
    }

    public void setPartitions(int[] partitions) {
        this.partitions = partitions != null ? partitions.clone() : new int[0];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberMapping that = (MemberMapping) o;
        return Objects.equals(member, that.member) && Arrays.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(member);
        result = 31 * result + Arrays.hashCode(partitions);
        return result;
    }

    @Override
    public String toString() {
        return "MemberMapping{" +
                "member='" + member + '\'' +
                ", partitions=" + Arrays.toString(partitions) +
                '}';
    }
}
