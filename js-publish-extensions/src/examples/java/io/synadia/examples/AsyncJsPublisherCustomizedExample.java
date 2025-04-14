// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.synadia.jnats.extension.AsyncJsPublisher;

public class AsyncJsPublisherCustomizedExample {

    public static final int COUNT = 100_0000;
    public static final String STREAM = "customStream";
    public static final String SUBJECT = "customSubject";

    public static void main(String[] args) {
        Options options = Options.builder()
            .server(Options.DEFAULT_URL)
            .connectionListener((connection, events) -> ExampleUtils.print("Connection Event:" + events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();

        try (Connection nc = Nats.connect(options)) {
            ExampleUtils.setupStream(nc, STREAM, SUBJECT);

            // --------------------------------------------------------------------------------
            // The listener is important for the developer to have a window in to the publishing.
            // see the ExamplePublishListener implementation
            // --------------------------------------------------------------------------------
            ExamplePublishListener publishListener = new ExamplePublishListener();

            // --------------------------------------------------------------------------------
            // These are the defaults from AsyncJsPublisher...
            // --------------------------------------------------------------------------------
            // public static final int DEFAULT_MAX_IN_FLIGHT = 50;
            // public static final int DEFAULT_REFILL_AMOUNT = 0;
            // public static final long DEFAULT_POLL_TIME = 100;
            // public static final long DEFAULT_PAUSE_TIME = 100;
            // public static final long DEFAULT_WAIT_TIMEOUT = DEFAULT_MAX_IN_FLIGHT * DEFAULT_POLL_TIME;
            // --------------------------------------------------------------------------------
            AsyncJsPublisher.Builder builder =
                AsyncJsPublisher.builder(nc.jetStream())
                    .maxInFlight(250)
                    .resumeAmount(100)
                    .pollTime(50)
                    .holdPauseTime(150)
                    .waitTimeout(3000)
                    .publishListener(publishListener);

            // The publisher is AutoCloseable
            try (AsyncJsPublisher publisher = builder.start()) {
                for (int x = 1; x <= COUNT; x++) {
                    publisher.publishAsync(SUBJECT, ("data-" + x).getBytes());
                }

                while (publisher.preFlightSize() > 0 || publisher.currentInFlight() > 0) {
                    ExampleUtils.printStateThenWait(publisher, publishListener);
                }

                ExampleUtils.printState(publisher, publishListener);
            }
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }
}
