// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.dc;

import io.nats.client.JetStreamManagement;

import java.time.ZonedDateTime;

public class DirectConsumerBuilder {

    public static final long[] DEFAULT_BACKOFF_POLICY = {100, 500, 1000, 2000};

    public static final int DEFAULT_BATCH_SIZE = 50;

    JetStreamManagement jsm;
    String stream;
    String subject;
    int batch = DEFAULT_BATCH_SIZE;
    Long startSequence;
    ZonedDateTime startTime;
    long[] backoffPolicy = DEFAULT_BACKOFF_POLICY;

    public DirectConsumerBuilder(JetStreamManagement jsm, String stream, String subject) {
        this.jsm = jsm;
        this.stream = stream;
        this.subject = subject;
    }

    public DirectConsumerBuilder batch(int batch) {
        this.batch = batch;
        return this;
    }

    public DirectConsumerBuilder startSequence(long startSequence) {
        if (startTime != null) {
            throw new IllegalArgumentException("Start Sequence not valid when Start Time is provided");
        }
        this.startSequence = startSequence;
        return this;
    }

    public DirectConsumerBuilder startTime(ZonedDateTime startTime) {
        if (startSequence != null) {
            throw new IllegalArgumentException("Start Time not valid when Start Sequence is provided");
        }
        this.startTime = startTime;
        return this;
    }

    public DirectConsumerBuilder backoffPolicy(long[] backoffPolicy) {
        if (backoffPolicy == null || backoffPolicy.length == 0) {
            this.backoffPolicy = DEFAULT_BACKOFF_POLICY;
        }
        else {
            this.backoffPolicy = backoffPolicy;
        }
        return this;
    }

    public DirectConsumer build() {
        return new DirectConsumer(this);
    }
}
