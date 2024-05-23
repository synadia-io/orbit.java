// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;

import java.util.concurrent.CompletableFuture;

public class Flight {
    final PreFlight pre;
    final long publishTime;
    final CompletableFuture<PublishAck> publishAckFuture;

    public Flight(PreFlight pre, CompletableFuture<PublishAck> publishAckFuture) {
        publishTime = System.currentTimeMillis();
        this.pre = pre;
        this.publishAckFuture = publishAckFuture;
    }

    public long getPublishTime() {
        return publishTime;
    }

    public CompletableFuture<PublishAck> getPublishAckFuture() {
        return publishAckFuture;
    }

    public String getId() {
        return pre.id;
    }

    public String getSubject() {
        return pre.subject;
    }

    public Headers getHeaders() {
        return pre.headers;
    }

    public byte[] getBody() {
        return pre.body;
    }

    public PublishOptions getOptions() {
        return pre.options;
    }
}
