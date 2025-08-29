// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;

import java.math.BigInteger;
import java.util.Map;

import static io.synadia.counter.CounterUtils.*;

public class CounterEntry {
    private final String subject;
    private final BigInteger value;
    private final BigInteger lastIncrement;
    private final Map<String, Map<String, BigInteger>> sources;

    CounterEntry(MessageInfo mi) {
        if (!mi.isMessage()) {
            throw invalidCounterMessage(null);
        }

        subject = mi.getSubject();
        try {
            value = extractVal(mi.getData());
        }
        catch (RuntimeException e) {
            throw invalidCounterMessage(e);
        }

        Headers h = mi.getHeaders();
        if (h == null) {
            throw invalidCounterMessage(null);
        }

        String temp = h.getFirst(CounterUtils.INCREMENT_HEADER);
        if (temp == null) {
            throw invalidCounterMessage(null);
        }
        lastIncrement = extractLastIncrement(temp);

        sources = extractSources(h.getFirst(CounterUtils.SOURCES_HEADER));
    }

    private static RuntimeException invalidCounterMessage(Exception e) {
        return new RuntimeException("Message is not a counter message", e);
    }

    @NonNull
    public String getSubject() {
        return subject;
    }

    @NonNull
    public BigInteger getValue() {
        return value;
    }

    @NonNull
    public BigInteger getLastIncrement() {
        return lastIncrement;
    }

    @NonNull
    public Map<String, Map<String, BigInteger>> getSources() {
        return sources;
    }

    @Override
    public String toString() {
        return "CounterEntry{" +
            "subject=\"" + subject + '\"' +
            ", value=" + value +
            ", lastIncrement=" + lastIncrement +
            ", sources=" + sources +
            '}';
    }
}
