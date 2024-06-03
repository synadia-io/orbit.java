// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;

/**
 * This object represents a message in the AsyncJsPublisher workflow
 * after being published.
 */
public class PostFlight {
    public final InFlight inFlight;
    public final PublishAck publishAck;
    public final boolean completedExceptionally;
    public final boolean timeout;
    public final boolean expectationFailed;
    public final Throwable cause;

    public PostFlight(InFlight inFlight, PublishAck publishAck) {
        this.inFlight = inFlight;
        this.publishAck = publishAck;
        this.completedExceptionally = false;
        this.timeout = false;
        this.expectationFailed = false;
        this.cause = null;
    }

    public PostFlight(InFlight inFlight, Throwable cause) {
        this.inFlight = inFlight;
        this.publishAck = null;
        this.completedExceptionally = true;
        this.timeout = false;
        this.expectationFailed = false;
        this.cause = cause;
    }

    public PostFlight(InFlight inFlight, boolean timeout, boolean expectationFailed, Throwable cause) {
        this.inFlight = inFlight;
        this.publishAck = null;
        this.completedExceptionally = true;
        this.timeout = timeout;
        this.expectationFailed = expectationFailed;
        this.cause = cause;
    }

    public String getMessageId() {
        return inFlight.preFlight.messageId;
    }

    public String getSubject() {
        return inFlight.preFlight.subject;
    }

    public Headers getHeaders() {
        return inFlight.preFlight.headers;
    }

    public byte[] getBody() {
        return inFlight.preFlight.body;
    }

    public PublishOptions getOptions() {
        return inFlight.preFlight.options;
    }

    public PublishAck getPublishAck() {
        return publishAck;
    }

    public boolean isCompletedExceptionally() {
        return completedExceptionally;
    }

    public boolean isTimeout() {
        return timeout;
    }

    public boolean isExpectationFailed() {
        return expectationFailed;
    }

    public Throwable getCause() {
        return cause;
    }
}
