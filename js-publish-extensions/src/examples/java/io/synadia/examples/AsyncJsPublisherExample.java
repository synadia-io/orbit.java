// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.synadia.jnats.extension.AsyncJsPublisher;
import io.synadia.jnats.extension.PublishRetryConfig;

public class AsyncJsPublisherExample {

    public static final int COUNT = 100_000;
    public static final String STREAM = "exampleStream";
    public static final String SUBJECT = "exampleSubject";

    public static final boolean USE_RETRIER = false;  // set this to true to have each publish use retry logic

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

            AsyncJsPublisher.Builder builder =
                AsyncJsPublisher.builder(nc.jetStream())
                    .publishListener(publishListener);

            // --------------------------------------------------------------------------------
            // If you want to use retrying for publishing, you must give a Retry Config
            // --------------------------------------------------------------------------------
            if (USE_RETRIER) {
                builder.retryConfig(PublishRetryConfig.DEFAULT_CONFIG);
            }

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
