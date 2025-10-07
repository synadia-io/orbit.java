// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counters;

import io.nats.client.api.MessageInfo;
import io.nats.client.support.Status;
import org.jspecify.annotations.Nullable;

abstract class CounterResponse {
    protected final MessageInfo mi;

    CounterResponse(MessageInfo mi) {
        this.mi = mi;
    }

    @Nullable
    public Status getStatus() {
        return mi.getStatus();
    }

    /**
     * Whether this response is a status message
     * @return true if this CounterEntry is a status message
     */
    public boolean isStatus() {
        return mi.isStatus();
    }

    /**
     * Whether this response is a status message and is a direct EOB status
     * @return true if this CounterEntry is a status message and is a direct EOB status
     */
    public boolean isEobStatus() {
        return mi.isEobStatus();
    }

    /**
     * Whether this response is a status message and is an error status
     * @return true if this CounterEntry is a status message and is an error status
     */
    public boolean isErrorStatus() {
        return mi.isErrorStatus();
    }
}
