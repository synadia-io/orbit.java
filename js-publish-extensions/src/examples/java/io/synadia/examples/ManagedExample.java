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
import io.synadia.jnats.extension.Flight;
import io.synadia.jnats.extension.ManagedAsyncJsPublisher;
import io.synadia.jnats.extension.PublisherListener;
import io.synadia.jnats.extension.RetryConfig;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class ManagedExample {

    public static final int COUNT = 100_000;
    public static final String STREAM = "managed";
    public static final String SUBJECT = "managed_subject";
    public static final boolean useRetrier = true;

    public static void main(String[] args) {
        Options options = Options.builder()
            .connectionListener((connection, events) -> print("Connection Event:" + events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();

        try (Connection nc = Nats.connect(options)) {
            setupStream(nc);

            PublisherListener publisherListener = new ExamplePublisherListener();

            ManagedAsyncJsPublisher.Builder builder =
                ManagedAsyncJsPublisher.builder(nc.jetStream())
                    .publisherListener(publisherListener);

            if (useRetrier) {
                builder.retryConfig(RetryConfig.DEFAULT_CONFIG);
            }

            // the publisher is AutoCloseable
            try (ManagedAsyncJsPublisher managed = builder.start()) {
                for (int x = 0; x < COUNT; x++) {
                    managed.publishAsync(SUBJECT, ("data-" + x).getBytes());
                }

                while (managed.preFlightSize() > 0) {
                    System.out.println(publisherListener);
                    //noinspection BusyWait
                    Thread.sleep(1000);
                }
                System.out.println(publisherListener);
            }
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
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

    static class ExamplePublisherListener implements PublisherListener {
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
                print("completedExceptionally", new String(flight.body), e.toString());
            }
        }

        @Override
        public void timeout(Flight flight) {
            try {
                timedOut.incrementAndGet();
                flight.publishAckFuture.get();
            }
            catch (Exception e) {
                print("timeout", new String(flight.body), e.toString());
            }
        }
    }

    private static void print(String... strings) {
        System.out.println(String.join(" | ", strings));
    }
}
