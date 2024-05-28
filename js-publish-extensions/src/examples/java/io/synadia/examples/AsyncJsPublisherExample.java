// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.synadia.jnats.extension.AsyncJsPublishListener;
import io.synadia.jnats.extension.AsyncJsPublisher;
import io.synadia.jnats.extension.Flight;
import io.synadia.retrier.RetryConfig;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncJsPublisherExample {

    public static final int COUNT = 100_000;
    public static final String STREAM = "managed";
    public static final String SUBJECT = "managed_subject";

    public static final boolean USE_RETRIER = false;  // set this to true to have each publish use retry logic
    public static final boolean BUILT_IN_START = true; // set this to false in order to demonstrate the custom start

    public static void main(String[] args) {
        Options options = Options.builder()
            .connectionListener((connection, events) -> print("Connection Event:" + events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();

        try (Connection nc = Nats.connect(options)) {
            setupStream(nc);

            // --------------------------------------------------------------------------------
            // The listener is important for the developer to have a window in to the publishing.
            // see the ExamplePublishListener implementation
            // --------------------------------------------------------------------------------
            AsyncJsPublishListener publishListener = new ExamplePublishListener();

            AsyncJsPublisher.Builder builder =
                AsyncJsPublisher.builder(nc.jetStream())
                    .publishListener(publishListener);

            // --------------------------------------------------------------------------------
            // If you want to use retrying for publishing, you must give a Retry Config
            // --------------------------------------------------------------------------------
            if (USE_RETRIER) {
                builder.retryConfig(RetryConfig.DEFAULT_CONFIG);
            }

            // --------------------------------------------------------------------------------
            // Built In Start or Custom Start
            // --------------------------------------------------------------------------------
            // Initially, you can just use the built-in start. You can see the code for that is much simpler
            // all the management of threads and the executor service is taken care of.
            // --------------------------------------------------------------------------------
            // Since we can envision developers wanting more control, the non-built-in code path
            // demonstrates the developer supplying its own threads for running
            // the event loops and for providing the ExecutorService for notification.
            // The custom code here is essentially the same as the built-in, but shows how the
            // developer can do it themselves.
            // --------------------------------------------------------------------------------
            if (BUILT_IN_START) {
                // the publisher is AutoCloseable
                try (AsyncJsPublisher publisher = builder.start()) {
                    publish(publisher, publishListener);
                }
            }
            // --------------------------------------------------------------------------------
            else {
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

                // same publish logic as the built-in start
                publish(publisher, publishListener);

                // if you have a custom start, you probably want some custom closing
                // again, the example mimics what the built-in does
                // don't forget to call the publisher close, because it does some stuff
                publisher.close();
                notificationExecutorService.shutdown();
                if (!publisher.getPublishRunnerDoneLatch().await(publisher.getPollTime(), TimeUnit.MILLISECONDS)) {
                    publishRunnerThread.interrupt();
                }
                if (!publisher.getFlightsRunnerDoneLatch().await(publisher.getPollTime(), TimeUnit.MILLISECONDS)) {
                    flightsRunnerThread.interrupt();
                }
            }
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }

    private static void publish(AsyncJsPublisher publisher, AsyncJsPublishListener publishListener) throws InterruptedException {
        for (int x = 0; x < COUNT; x++) {
            publisher.publishAsync(SUBJECT, ("data-" + x).getBytes());
        }

        while (publisher.preFlightSize() > 0) {
            System.out.println(publishListener);
            //noinspection BusyWait
            Thread.sleep(1000);
        }

        while (publisher.inFlightSize() > 0) {
            System.out.println(publishListener);
            //noinspection BusyWait
            Thread.sleep(1000);
        }

        System.out.println(publishListener);
    }

    private static void setupStream(Connection nc) {
        try {
            nc.jetStreamManagement().deleteStream(STREAM);
        }
        catch (Exception ignore) {}
        try {
            System.out.println("Creating Stream @ " + System.currentTimeMillis());
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .storageType(StorageType.File)
                .build());
        }
        catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    static class ExamplePublishListener implements AsyncJsPublishListener {
        public AtomicLong published = new AtomicLong();
        public AtomicLong acked = new AtomicLong();
        public AtomicLong exceptioned = new AtomicLong();
        public AtomicLong timedOut = new AtomicLong();

        @Override
        public String toString() {
            return "published=" + published +
                ", acked=" + acked +
                ", exceptioned=" + exceptioned +
                ", timed out=" + timedOut;
        }

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
                print("completedExceptionally", new String(flight.getBody()), e.toString());
            }
        }

        @Override
        public void timeout(Flight flight) {
            try {
                timedOut.incrementAndGet();
                flight.publishAckFuture.get();
            }
            catch (Exception e) {
                print("timeout", new String(flight.getBody()), e.toString());
            }
        }
    }

    private static void print(String... strings) {
        System.out.println(String.join(" | ", strings));
    }
}
