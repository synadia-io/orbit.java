// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;

import static io.synadia.counter.CounterUtils.extractVal;

public class CounterValueResponse extends CounterResponse {

    CounterValueResponse(MessageInfo mi) {
        super(mi);
    }

    /**
     * Whether this CounterValue is a regular value as opposed to an error/status
     * @return true if the CounterValue is a regular value
     */
    public boolean isValue() {
        return mi.isMessage();
    }

    @Nullable
    public BigInteger getValue() {
        return mi.isMessage() ? new BigInteger(extractVal(mi.getData())) : null;
    }

    @Override
    public String toString() {
        return "CounterValueResponse{ " + (isValue() ? getValue() : getStatus() ) + " }";
    }
}
