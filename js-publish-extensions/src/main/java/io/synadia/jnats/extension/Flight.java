// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
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
public class Flight {
    public final long publishTime;
    public final CompletableFuture<PublishAck> publishAckFuture;
    public final PreFlight preFlight;

    public Flight(CompletableFuture<PublishAck> publishAckFuture, PreFlight preFlight) {
        publishTime = System.currentTimeMillis();
        this.publishAckFuture = publishAckFuture;
        this.preFlight = preFlight;
    }

    public long getPublishTime() {
        return publishTime;
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

    public CompletableFuture<Flight> getFlightFuture() {
        return preFlight.flightFuture;
    }

    @Override
    public String toString() {
        return "Flight{" +
            "messageId='" + preFlight.messageId + '\'' +
            ", subject='" + preFlight.subject + '\'' +
            ", body=" + (preFlight.body == null ? "null" : new String(preFlight.body)) +
            ", publishTime=" + publishTime +
            ", publishAckFuture.isDone()=" + publishAckFuture.isDone() +
            '}';
    }
}
