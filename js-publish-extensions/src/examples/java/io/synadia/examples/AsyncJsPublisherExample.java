// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.synadia.jnats.extension.AsyncJsPublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AsyncJsPublisherExample {

    // --------------------------------------------------------------------------------
    // Example general configuration
    // --------------------------------------------------------------------------------
    public static final String STREAM = "exampleStream";
    public static final String SUBJECT = "exampleSubject";
    public static final int PUBLISH_COUNT = 1_000_000;

    // --------------------------------------------------------------------------------
    // AsyncJsPublisher configuration
    // These are the defaults if you don't manually set them in the builder.
    // You can play with the values and see how it affects the run
    // --------------------------------------------------------------------------------
    // public static final int DEFAULT_MAX_IN_FLIGHT = 50;
    // public static final int DEFAULT_REFILL_AMOUNT = 0;
    // public static final long DEFAULT_POLL_TIME = 100;
    // public static final long DEFAULT_PAUSE_TIME = 100;
    // public static final long DEFAULT_WAIT_TIMEOUT = DEFAULT_MAX_IN_FLIGHT * DEFAULT_POLL_TIME;
    // --------------------------------------------------------------------------------
    public static final int MAX_IN_FLIGHT = 10000;
    public static final int RESUME_AMOUNT = 1000;
    public static final long POLL_TIME = 50;
    public static final long PUBLISH_PAUSE_TIME = 100;
    public static final long WAIT_TIMEOUT = 2500;

    public static void main(String[] args) {
        Options options = Options.builder()
            .server(Options.DEFAULT_URL)
            .connectionListener((connection, events) -> ExampleUtils.print("Connection Event", events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();

        try (Connection nc = Nats.connect(options)) {
            ExampleUtils.setupStream(nc, STREAM, SUBJECT);
            JetStream js = nc.jetStream();

            // --------------------------------------------------------------------------------
            // Build the AsyncJsPublisher...
            // --------------------------------------------------------------------------------
            // The listener is important for the developer to have a window into the publishing
            // It will be called as an executor task
            // --------------------------------------------------------------------------------
            ExamplePublishListener publishListener = new ExamplePublishListener();
            AsyncJsPublisher.Builder builder =
                AsyncJsPublisher.builder(js)
                    .maxInFlight(MAX_IN_FLIGHT)
                    .resumeAmount(RESUME_AMOUNT)
                    .pollTime(POLL_TIME)
                    .publishPauseTime(PUBLISH_PAUSE_TIME)
                    .waitTimeout(WAIT_TIMEOUT)
                    .publishListener(publishListener);
            AsyncJsPublisher publisher = builder.start();

            // --------------------------------------------------------------------------------
            // Example logic
            // --------------------------------------------------------------------------------

            // --------------------------------------------------------------------------------
            // Add the entire count of messages to the publisher, it will put them in a queue
            // Usually publishing will happen more organically, this is just for the example.
            // --------------------------------------------------------------------------------
            for (int x = 1; x <= PUBLISH_COUNT; x++) {
                publisher.publishAsync(SUBJECT, ("data-" + x).getBytes());
            }

            // --------------------------------------------------------------------------------
            // Once the listener detects that all the messages have been actually published,
            // move to the next phase of the example.
            // --------------------------------------------------------------------------------
            while (publishListener.publishedCount.get() < PUBLISH_COUNT) {
                ExampleUtils.printStatus(publisher, publishListener, false);
                //noinspection BusyWait
                Thread.sleep(500);
            }

            // --------------------------------------------------------------------------------
            // Call stop with true for drain this tells the publisher that we are done with it
            // but to drain (finish publishing) and finish handling in-flight messages.
            // --------------------------------------------------------------------------------
            // There is also a no-parameter stop() that does drain.
            // --------------------------------------------------------------------------------
            // If you call stop without drain == true, the publisher
            // will finish any publish or in-flight check the current loop is in the middle of
            // but then stop after that with unfinished work.
            // --------------------------------------------------------------------------------
            publisher.stop(true);

            // --------------------------------------------------------------------------------
            // When we call stop, there might be up to one more poll (POLL_TIME) of the
            // pre-flight (unpublished) queue, but after that, the publish thread/loop will
            // finish and the PublishRunnerDoneFuture will complete.
            // --------------------------------------------------------------------------------
            CompletableFuture<Void> future = publisher.getPublishRunnerDoneFuture();
            future.get(POLL_TIME + 10, TimeUnit.MILLISECONDS);

            // --------------------------------------------------------------------------------
            // When you try to publish to a stopped publisher, you get an exception
            // --------------------------------------------------------------------------------
            try {
                System.out.println("Attempting to publish after stop should throw an exception...");
                publisher.publishAsync(SUBJECT, "should fail".getBytes());
                System.out.println("SHOULD HAVE EXCEPTIONED!");
            }
            catch (IllegalStateException e) {
                System.out.println("Got exception as expected: " + e);
            }

            // --------------------------------------------------------------------------------
            // This future lets us know when all the in-flight messages are acked.
            // --------------------------------------------------------------------------------
            System.out.println("Waiting for the flight runner to complete processing publish acks...");
            publisher.getFlightsRunnerDoneFuture().get(11, TimeUnit.MINUTES);// should be done much sooner
            ExampleUtils.printStatus(publisher, publishListener, true);

            // --------------------------------------------------------------------------------
            // This publisher is AutoCloseable, but we didn't start it in a try-resources,
            // so just close it manually.
            // --------------------------------------------------------------------------------
            publisher.close();
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }
}
