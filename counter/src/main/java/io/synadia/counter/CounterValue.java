// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import io.nats.client.support.Status;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;

import static io.synadia.counter.CounterUtils.extractVal;

public class CounterValue {
    public final String subject;
    public final BigInteger value;
    public final Status status;

    CounterValue(MessageInfo mi) {
        this.status = mi.getStatus();
        if (mi.isMessage()) {
            this.subject = mi.getSubject();
            this.value = new BigInteger(extractVal(mi.getData()));
        }
        else {
            this.subject = "";
            this.value = BigInteger.ZERO;
        }
    }

    /**
     * Whether this CounterValue is a regular value as opposed to an error/status
     * @return true if the CounterEntry is a regular value
     */
    public boolean isValue() {
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

    @Nullable
    public Status getStatus() {
        return status;
    }

    @Override
    public String toString() {
        if (isValue()) {
            return "CounterValue{" +
                "subject='" + subject + '\'' +
                ", value=" + value +
                '}';
        }

        return "CounterValue{" +
            "status=" + status +
            '}';
    }
}
