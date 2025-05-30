// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.support.*;

import java.util.Objects;

public class KeyOrValue implements JsonSerializable {
    public final String part1;
    public final String part2;
    public final boolean isKey;

    public KeyOrValue(String part1, String part2, boolean isKey) {
        this.part1 = part1;
        this.part2 = part2;
        this.isKey = isKey;
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof KeyOrValue)) return false;

        KeyOrValue that = (KeyOrValue) o;
        return isKey == that.isKey && Objects.equals(part1, that.part1) && Objects.equals(part2, that.part2);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(part1);
        result = 31 * result + Objects.hashCode(part2);
        result = 31 * result + Boolean.hashCode(isKey);
        return result;
    }

    public KeyOrValue(byte[] jsonBytes) {
        try {
            JsonValue jv = JsonParser.parse(jsonBytes);
            this.isKey = JsonValueUtils.readBoolean(jv, "isKey", false);
            part1 = JsonValueUtils.readString(jv, "part1");
            part2 = JsonValueUtils.readString(jv, "part2");
        }
        catch (JsonParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toJson() {
        return JsonValueUtils.mapBuilder()
            .put("isKey", isKey)
            .put("part1", part1)
            .put("part2", part2)
            .toJson();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
