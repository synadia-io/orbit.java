// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.synadia.jnats.extension.AsyncJsPublisher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncJsPublisherCustomThreadsExample {

    public static final int COUNT = 100_000;
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

            AsyncJsPublisher.Builder builder =
                AsyncJsPublisher.builder(nc.jetStream())
                    .publishListener(publishListener);

            // --------------------------------------------------------------------------------
            // Custom Start
            // --------------------------------------------------------------------------------
            // Since we can envision developers wanting more control, the non-built-in code path
            // demonstrates the developer supplying its own threads for running
            // the event loops and for providing the ExecutorService for notification.
            // The custom code here is essentially the same as the built-in, but shows how the
            // developer can do it themselves.
            // --------------------------------------------------------------------------------
            // custom notification executor
            ExecutorService notificationExecutorService = Executors.newFixedThreadPool(1);
            builder.notificationExecutorService(notificationExecutorService);

            AsyncJsPublisher publisher = builder.build();

            // this custom start mimics what the built-in does but
            // shows how to access the publish / flights runner Runnable(s)
            Thread publishRunnerThread = new Thread(publisher::publishRunner);
            publishRunnerThread.start();
            Thread flightsRunnerThread = new Thread(publisher::flightsRunner);
            flightsRunnerThread.start();

            // --------------------------------------------------------------------------------
            // example logic
            // --------------------------------------------------------------------------------
            for (int x = 1; x <= COUNT; x++) {
                publisher.publishAsync(SUBJECT, ("data-" + x).getBytes());
            }

            while (publisher.preFlightSize() > 0 || publisher.currentInFlight() > 0) {
                ExampleUtils.printStateThenWait(publisher, publishListener);
            }

            ExampleUtils.printState(publisher, publishListener);

            // --------------------------------------------------------------------------------
            // if you have a custom start, you probably want some custom closing
            // again, the example mimics what the built-in does
            // don't forget to call the publisher close, because it does some stuff
            // --------------------------------------------------------------------------------
            publisher.close();
            notificationExecutorService.shutdown();
            if (!publisher.getPublishRunnerDoneLatch().await(publisher.getPollTime(), TimeUnit.MILLISECONDS)) {
                publishRunnerThread.interrupt();
            }
            if (!publisher.getFlightsRunnerDoneLatch().await(publisher.getPollTime(), TimeUnit.MILLISECONDS)) {
                flightsRunnerThread.interrupt();
            }
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }
}
