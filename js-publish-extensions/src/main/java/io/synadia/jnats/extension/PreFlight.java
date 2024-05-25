// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.util.concurrent.CompletableFuture;

/**
 * This object represents the message as given to the AsyncJsPublisher
 * and is carried through until it's actually published, at which time it's converted
 * to a Flight.
 */
public class PreFlight {
    public final String id;
    public final String subject;
    public final Headers headers;
    public final byte[] body;
    public final PublishOptions options;
    public final CompletableFuture<Flight> flightFuture;

    public PreFlight(String id, String subject, Headers headers, byte[] body, PublishOptions options) {
        this.id = id;
        this.subject = subject;
        this.headers = headers;
        this.body = body;
        this.options = options;
        flightFuture = new CompletableFuture<>();
    }

    protected PreFlight(PreFlight preFlight) {
        this.id = preFlight.id;
        this.subject = preFlight.subject;
        this.headers = preFlight.headers;
        this.body = preFlight.body;
        this.options = preFlight.options;
        flightFuture = preFlight.flightFuture;
    }

    public String getId() {
        return id;
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

    public CompletableFuture<Flight> getFlightFuture() {
        return flightFuture;
    }
}
