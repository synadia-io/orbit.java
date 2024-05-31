// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.jnats.extension.AsyncJsPublishListener;
import io.synadia.jnats.extension.InFlight;
import io.synadia.jnats.extension.PostFlight;

import java.util.concurrent.atomic.AtomicLong;

class ExamplePublishListener implements AsyncJsPublishListener {
    public AtomicLong published = new AtomicLong();
    public AtomicLong acked = new AtomicLong();
    public AtomicLong exceptioned = new AtomicLong();
    public AtomicLong timedOut = new AtomicLong();

    @Override
    public void published(InFlight flight) {
        published.incrementAndGet();
    }

    @Override
    public void acked(PostFlight postFlight) {
        acked.incrementAndGet();
    }

    @Override
    public void completedExceptionally(PostFlight postFlight) {
        exceptioned.incrementAndGet();
        if (postFlight.expectationFailed) {
            ExampleUtils.print("Expectation Failed", new String(postFlight.getBody()), postFlight.cause);
        }
        else {
            ExampleUtils.print("Completed Exceptionally", new String(postFlight.getBody()), postFlight.cause);
        }
    }

    @Override
    public void timeout(PostFlight postFlight) {
        timedOut.incrementAndGet();
        ExampleUtils.print("Timed-out", new String(postFlight.getBody()) );
    }
}
