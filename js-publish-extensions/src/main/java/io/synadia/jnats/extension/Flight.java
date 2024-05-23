// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.api.PublishAck;

import java.util.concurrent.CompletableFuture;

/**
 *
 */
public class Flight extends PreFlight {
    public final long publishTime;
    public final CompletableFuture<PublishAck> publishAckFuture;
    public long elapsed;

    public Flight(PreFlight preFlight, CompletableFuture<PublishAck> publishAckFuture) {
        super(preFlight);
        publishTime = System.currentTimeMillis();
        this.publishAckFuture = publishAckFuture;
        elapsed = -1;
    }

    public long getPublishTime() {
        return publishTime;
    }

    public CompletableFuture<PublishAck> getPublishAckFuture() {
        return publishAckFuture;
    }
}
