// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.jnats.extension.AsyncJsPublishListener;
import io.synadia.jnats.extension.Flight;

import java.util.concurrent.atomic.AtomicLong;

class ExamplePublishListener implements AsyncJsPublishListener {
    public AtomicLong published = new AtomicLong();
    public AtomicLong acked = new AtomicLong();
    public AtomicLong exceptioned = new AtomicLong();
    public AtomicLong timedOut = new AtomicLong();

    @Override
    public void published(Flight flight) {
        published.incrementAndGet();
    }

    @Override
    public void acked(Flight flight) {
        acked.incrementAndGet();
    }

    @Override
    public void completedExceptionally(Flight flight) {
        try {
            exceptioned.incrementAndGet();
            flight.publishAckFuture.get();
        }
        catch (Exception e) {
            ExampleUtils.print("completedExceptionally", new String(flight.getBody()), e.toString());
        }
    }

    @Override
    public void timeout(Flight flight) {
        try {
            timedOut.incrementAndGet();
            flight.publishAckFuture.get();
        }
        catch (Exception e) {
            ExampleUtils.print("timeout", new String(flight.getBody()), e.toString());
        }
    }
}
