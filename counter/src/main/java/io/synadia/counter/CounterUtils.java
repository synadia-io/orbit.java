// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public final class CounterUtils {

    public static final String INCREMENT_HEADER = "Nats-Incr";
    public static final String SOURCES_HEADER = "Nats-Counter-Sources";

    public static BigInteger extractVal(byte @NonNull [] valBytes) {
        String s = new String(valBytes);
        // it's just faster to parse this manually
        // {"val":"-123"}
        // don't want to assume anything about how the json is formatted/spaced
        int colonAt = s.indexOf(':');
        int numberStart = s.indexOf('"', colonAt + 1) + 1;
        int lastQuote = s.lastIndexOf('"');
        return new BigInteger(s.substring(numberStart, lastQuote).trim());
    }

    public static BigInteger extractLastIncrement(@NonNull String numberString) {
        return new BigInteger(numberString);
    }

    public static Map<String, Map<String, BigInteger>> extractSources(@Nullable String json) {
        Map<String, Map<String, BigInteger>> sources = new HashMap<>();
        if (json != null) {
            try {
                JsonValue v = JsonParser.parse(json);
                if (v.map != null) {
                    for (Map.Entry<String, JsonValue> entry : v.map.entrySet()) {
                        String key = entry.getKey();
                        Map<String, JsonValue> value = entry.getValue().map;
                        if (value != null) {
                            Map<String, BigInteger> sourceValue = new HashMap<>();
                            sources.put(key, sourceValue);
                            for (Map.Entry<String, JsonValue> entry2 : value.entrySet()) {
                                String key2 = entry2.getKey();
                                JsonValue value2 = entry2.getValue();
                                sourceValue.put(key2, new BigInteger(value2.string));
                            }
                        }
                    }
                }
            }
            catch (JsonParseException e) {
                throw new RuntimeException(e);
            }
        }
        return sources;
    }
}
