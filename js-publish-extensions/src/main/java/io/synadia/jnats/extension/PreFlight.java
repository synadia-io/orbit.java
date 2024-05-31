// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.util.concurrent.CompletableFuture;

/**
 * This object represents the message as given to the AsyncJsPublisher
 * and is carried through until it's actually published, at which time it's converted
 * to a InFlight.
 */
public class PreFlight {
    public final String messageId;
    public final String subject;
    public final Headers headers;
    public final byte[] body;
    public final PublishOptions options;
    public final CompletableFuture<InFlight> flightFuture;

    public PreFlight(String messageId, String subject, Headers headers, byte[] body, PublishOptions options) {
        this.messageId = messageId;
        this.subject = subject;
        this.headers = headers;
        this.body = body;
        this.options = options;
        flightFuture = new CompletableFuture<>();
    }

    protected PreFlight(PreFlight preFlight) {
        this.messageId = preFlight.messageId;
        this.subject = preFlight.subject;
        this.headers = preFlight.headers;
        this.body = preFlight.body;
        this.options = preFlight.options;
        flightFuture = preFlight.flightFuture;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getSubject() {
        return subject;
    }

    public Headers getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }

    public PublishOptions getOptions() {
        return options;
    }

    public CompletableFuture<InFlight> getFlightFuture() {
        return flightFuture;
    }

    @Override
    public String toString() {
        return "PreFlight{" +
            "messageId='" + messageId + '\'' +
            ", subject='" + subject + '\'' +
            ", body=" + (body == null ? "null" : new String(body)) +
            '}';
    }
}
