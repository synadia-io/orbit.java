// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;

import java.util.concurrent.CompletableFuture;

public class PreFlight {

    final String id;
    final String subject;
    final Headers headers;
    final byte[] body;
    final PublishOptions options;
    final CompletableFuture<Flight> flightFuture;

    PreFlight(String id, String subject, Headers headers, byte[] body, PublishOptions options) {
        this.id = id;
        this.subject = subject;
        this.headers = headers;
        this.body = body;
        this.options = options;
        flightFuture = new CompletableFuture<>();
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
}
