// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import io.nats.client.support.Status;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static io.synadia.counter.CounterUtils.extractIncrement;
import static io.synadia.counter.CounterUtils.extractVal;

public class CounterEntry {
    public final String subject;
    public final BigInteger value;
    public final BigInteger lastIncrement;
    public final Map<String, BigInteger> sources;
    public final Status status;

    CounterEntry(MessageInfo mi) {
        this.status = mi.getStatus();
        this.sources = new HashMap<>();
        if (mi.isMessage()) {
            this.subject = mi.getSubject();
            this.value = new BigInteger(extractVal(mi.getData()));
            this.lastIncrement = new BigInteger(extractIncrement(mi.getHeaders()));
        }
        else {
            this.subject = "";
            this.value = BigInteger.ZERO;
            this.lastIncrement = BigInteger.ZERO;
        }
    }

    /**
     * Whether this CounterEntry is a regular entry
     * @return true if the CounterEntry is a regular entry
     */
    public boolean isEntry() {
        return status == null;
    }

    /**
     * Whether this CounterEntry is a status message
     * @return true if this CounterEntry is a status message
     */
    public boolean isStatus() {
        return status != null;
    }

    /**
     * Whether this CounterEntry is a status message and is a direct EOB status
     * @return true if this CounterEntry is a status message and is a direct EOB status
     */
    public boolean isEobStatus() {
        return status != null && status.isEob();
    }

    /**
     * Whether this CounterEntry is a status message and is an error status
     * @return true if this CounterEntry is a status message and is an error status
     */
    public boolean isErrorStatus() {
        return status != null && !status.isEob();
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
    public Map<String, BigInteger> getSources() {
        return sources;
    }

    @Nullable
    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        if (isEntry()) {
            return "CounterEntry{" +
                "subject='" + subject + '\'' +
                ", value=" + value +
                ", lastIncrement=" + lastIncrement +
                ", sources=" + sources +
                '}';
        }

        return "CounterEntry{" +
            "status=" + status +
            '}';
    }
}
