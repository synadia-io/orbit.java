// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.jnats.extension.AsyncJsPublishListener;
import io.synadia.jnats.extension.InFlight;
import io.synadia.jnats.extension.PostFlight;

import java.util.concurrent.atomic.AtomicLong;

public class ExamplePublishListener implements AsyncJsPublishListener {
    public AtomicLong published = new AtomicLong();
    public AtomicLong acked = new AtomicLong();
    public AtomicLong exceptioned = new AtomicLong();
    public AtomicLong timedOut = new AtomicLong();
    public AtomicLong start = new AtomicLong();
    public AtomicLong paused = new AtomicLong();
    public AtomicLong resumed = new AtomicLong();

    @Override
    public void published(InFlight flight) {
        start.compareAndSet(0, System.currentTimeMillis());
        published.incrementAndGet();
    }

    public long elapsed() {
        return System.currentTimeMillis() - start.get();
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
        ExampleUtils.print("Timed-out", new String(postFlight.getBody()));
    }

    @Override
    public void paused(int currentInFlight, int maxInFlight, int resumeAmount) {
        paused.incrementAndGet();
        ExampleUtils.print("Publishing paused." +
            " Current In Flight: " + currentInFlight +
            " Max In Flight: " + maxInFlight +
            " Resume Amount: " + resumeAmount);
    }

    @Override
    public void resumed(int currentInFlight, int maxInFlight, int resumeAmount) {
        resumed.incrementAndGet();
        ExampleUtils.print("Publishing resumed." +
            " Current In Flight: " + currentInFlight +
            " Max In Flight: " + maxInFlight +
            " Resume Amount: " + resumeAmount);
    }
}
