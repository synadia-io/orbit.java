// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;

import java.util.concurrent.CompletableFuture;

/**
 * This object represents a message in the AsyncJsPublisher workflow
 * after being published.
 */
public class InFlight {
    public final CompletableFuture<PublishAck> publishAckFuture;
    public final PreFlight preFlight;

    public InFlight(CompletableFuture<PublishAck> publishAckFuture, PreFlight preFlight) {
        this.publishAckFuture = publishAckFuture;
        this.preFlight = preFlight;
    }

    public CompletableFuture<PublishAck> getPublishAckFuture() {
        return publishAckFuture;
    }

    public String getMessageId() {
        return preFlight.messageId;
    }

    public String getSubject() {
        return preFlight.subject;
    }

    public Headers getHeaders() {
        return preFlight.headers;
    }

    public byte[] getBody() {
        return preFlight.body;
    }

    public PublishOptions getOptions() {
        return preFlight.options;
    }

    public CompletableFuture<InFlight> getFlightFuture() {
        return preFlight.inFlightFuture;
    }
}
