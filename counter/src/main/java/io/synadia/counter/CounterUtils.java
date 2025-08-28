// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import io.nats.client.impl.Headers;

import java.math.BigInteger;

public final class CounterUtils {

    public static final String INCREMENT_HEADER = "Nats-Incr";

    public static BigInteger extractVal(MessageInfo mi) {
        String s = new String(mi.getData());
        // {"val":"-123"}
        // don't want to assume anything about how the json is formatted/spaced
        int colonAt = s.indexOf(':');
        int numberStart = s.indexOf('"', colonAt + 1) + 1;
        int lastQuote = s.lastIndexOf('"');
        return new BigInteger(s.substring(numberStart, lastQuote).trim());
    }

    public static BigInteger extractIncrement(MessageInfo mi) {
        Headers h = mi.getHeaders();
        if (h == null) {
            return BigInteger.ZERO;
        }
        String inc = h.getFirst(INCREMENT_HEADER);
        return inc == null ? BigInteger.ZERO : new BigInteger(inc);
    }
}
