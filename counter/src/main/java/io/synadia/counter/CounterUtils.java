// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.impl.Headers;

public final class CounterUtils {

    public static final String INCREMENT_HEADER = "Nats-Incr";

    public static String extractVal(byte[] data) {
        String s = new String(data);
        // {"val":"-123"}
        // don't want to assume anything about how the json is formatted/spaced
        int colonAt = s.indexOf(':');
        int numberStart = s.indexOf('"', colonAt + 1) + 1;
        int lastQuote = s.lastIndexOf('"');
        return s.substring(numberStart, lastQuote).trim();
    }

    public static String extractIncrement(Headers h) {
        return  h == null ? "0" : h.getFirst(INCREMENT_HEADER);
    }
}
