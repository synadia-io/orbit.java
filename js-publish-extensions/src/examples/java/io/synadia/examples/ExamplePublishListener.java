// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.jnats.extension.AsyncJsPublishListener;
import io.synadia.jnats.extension.InFlight;
import io.synadia.jnats.extension.PostFlight;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ExamplePublishListener implements AsyncJsPublishListener {
    public AtomicLong startTime = new AtomicLong();
    public AtomicLong publishedCount = new AtomicLong();
    public AtomicLong ackedCount = new AtomicLong();
    public AtomicLong exceptionedCount = new AtomicLong();
    public AtomicLong timedOutCount = new AtomicLong();
    public AtomicLong pausedCount = new AtomicLong();
    public AtomicLong resumedCount = new AtomicLong();
    public AtomicBoolean paused = new AtomicBoolean(false);

    @Override
    public void published(InFlight flight) {
        publishedCount.incrementAndGet();
        startTime.compareAndSet(0, System.currentTimeMillis()); // only sets the first publish.
    }

    public long elapsed() {
        return System.currentTimeMillis() - startTime.get();
    }

    @Override
    public void acked(PostFlight postFlight) {
        ackedCount.incrementAndGet();
    }

    @Override
    public void completedExceptionally(PostFlight postFlight) {
        exceptionedCount.incrementAndGet();
        if (postFlight.expectationFailed) {
            ExampleUtils.print("Expectation Failed", new String(postFlight.getBody()), postFlight.cause);
        }
        else {
            ExampleUtils.print("Completed Exceptionally", new String(postFlight.getBody()), postFlight.cause);
        }

        // TODO THIS IS DEBUG TRYING TO FIGURE OUT HOW TO HANDLE 429
        //noinspection DataFlowIssue
        if (postFlight.cause.getMessage().contains("429")) {
            ExampleUtils.print("429", postFlight.getSubject(), postFlight.getMessageId(), new String(postFlight.getBody()));
            ExampleUtils.print("429", publishedCount.get(), ackedCount.get(), exceptionedCount.get(), timedOutCount.get());
            System.exit(-1);
        }
    }

    @Override
    public void timeout(PostFlight postFlight) {
        timedOutCount.incrementAndGet();
        ExampleUtils.print("Timed-out", new String(postFlight.getBody()));
    }

    @Override
    public void paused(int currentInFlight, int maxInFlight, int resumeAmount) {
        pausedCount.incrementAndGet();
        paused.set(true);
    }

    @Override
    public void resumed(int currentInFlight, int maxInFlight, int resumeAmount) {
        resumedCount.incrementAndGet();
        paused.set(false);
    }
}
