// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import org.jspecify.annotations.NonNull;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static io.synadia.counter.CounterUtils.extractIncrement;
import static io.synadia.counter.CounterUtils.extractVal;

public class CounterEntry {
    public final MessageInfo mi;

    CounterEntry(MessageInfo mi) {
        if (!mi.isMessage()) {
            throw new IllegalArgumentException("Message is not a counter message");
        }
        this.mi = mi;
    }

    @NonNull
    public String getSubject() {
        //noinspection DataFlowIssue we know it's not null
        return mi.getSubject();
    }

    @NonNull
    public BigInteger getValue() {
        return new BigInteger(extractVal(mi.getData()));
    }

    @NonNull
    public BigInteger getLastIncrement() {
        return new BigInteger(extractIncrement(mi.getHeaders()));
    }

    @NonNull
    public Map<String, BigInteger> getSources() {
        return new HashMap<>(); // TODO
    }

    @Override
    public String toString() {
        return "CounterEntry{" +
            "subject=\"" + getSubject() + '\"' +
            ", value=" + getValue() +
            ", lastIncrement=" + getLastIncrement() +
            ", sources=" + getSources() +
            '}';
    }
}
