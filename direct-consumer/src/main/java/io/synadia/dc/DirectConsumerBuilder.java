// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.dc;

import io.nats.client.JetStreamManagement;

import java.time.ZonedDateTime;

public class DirectConsumerBuilder {

    // private for now in case they change
    private static final long[] DEFAULT_BACKOFF_POLICY = {2000, 1000, 500, 100};
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int MAXIMUM_BATCH_SIZE = 1000;

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
        if (batch > MAXIMUM_BATCH_SIZE) {
            throw new IllegalArgumentException("The maximum batch size is " + MAXIMUM_BATCH_SIZE);
        }
        this.batch = batch < 1 ? DEFAULT_BATCH_SIZE : batch;
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
