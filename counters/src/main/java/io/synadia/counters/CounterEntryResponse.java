// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counters;

import io.nats.client.api.MessageInfo;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;

import static io.synadia.counters.CountersUtils.extractVal;

public class CounterEntryResponse extends CounterResponse {

    CounterEntryResponse(MessageInfo mi) {
        super(mi);
    }

    /**
     * Whether this CounterEntry is a regular entry as opposed to an error/status
     * @return true if the CounterEntry is a regular entry
     */
    public boolean isEntry() {
        return mi.isMessage();
    }

    @Nullable
    public BigInteger getValue() {
        return mi.isMessage() ? extractVal(mi.getData()) : null;
    }

    @Nullable
    public CounterEntry getEntry() {
        return mi.isMessage() ? new CounterEntry(mi) : null;
    }

    @Override
    public String toString() {
        return "CounterEntryResponse{ " + (isEntry() ? getEntry() : getStatus() ) + " }";
    }
}
