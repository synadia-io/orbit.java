// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.NUID;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.synadia.retrier.RetryConfig;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// TODO ? fine tune holding logic
// TODO ? Allow the user to define a maximum pre-flight queue size.

/**
 * This is the main entry point. Build an instance of this class and
 * publish to it instead of to the JetStream context.
 */
public class AsyncJsPublisher implements AutoCloseable {
    public static final int DEFAULT_MAX_IN_FLIGHT = 50;
    public static final int DEFAULT_REFILL_AMOUNT = 0;
    public static final long DEFAULT_POLL_TIME = 100;
    public static final long DEFAULT_PAUSE_TIME = 100;
    public static final long DEFAULT_WAIT_TIMEOUT = DEFAULT_MAX_IN_FLIGHT * DEFAULT_POLL_TIME;

    private static final PreFlight STOP_MARKER = new PreFlight("STOP", null, null, null, null);

    private final AtomicLong messageIdGenerator;
    private final JetStream js;
    private final String idPrefix;
    private final int maxInFlight;
    private final int refillAllowedAt;
    private final RetryConfig retryConfig;
    private final AsyncJsPublishListener publishListener;
    private final long pollTime;
    private final long holdPauseTime;
    private final long waitTimeout;
    private final LinkedBlockingQueue<PreFlight> preFlight;
    private final LinkedBlockingQueue<Flight> inFlight;
    private final AtomicBoolean notHolding;
    private final AtomicBoolean keepGoingPublishRunner;
    private final AtomicBoolean keepGoingFlightsRunner;
    private final AtomicBoolean draining;
    private final ExecutorService notificationExecutorService;
    private final boolean notificationExecutorServiceWasNotSupplied;
    private final AtomicReference<Thread> publishRunnerThread;
    private final AtomicReference<Thread> flightsRunnerThread;
    private final CountDownLatch publishRunnerDoneLatch;
    private final CountDownLatch flightsRunnerDoneLatch;

    private AsyncJsPublisher(Builder b) {
        messageIdGenerator = new AtomicLong(0);
        js = b.js;
        idPrefix = b.idPrefix;
        maxInFlight = b.maxInFlight;
        refillAllowedAt = b.refillAllowedAt;
        retryConfig = b.retryConfig;
        publishListener = b.publishListener;
        pollTime = b.pollTime;
        holdPauseTime = b.holdPauseTime;
        waitTimeout = b.waitTimeout;

        if (b.notificationExecutorService == null) {
            notificationExecutorService = Executors.newFixedThreadPool(1);
            notificationExecutorServiceWasNotSupplied = true;
        }
        else {
            notificationExecutorService = b.notificationExecutorService;
            notificationExecutorServiceWasNotSupplied = false;
        }

        preFlight = new LinkedBlockingQueue<>();
        inFlight = new LinkedBlockingQueue<>();
        notHolding = new AtomicBoolean(true);
        keepGoingPublishRunner = new AtomicBoolean(true);
        keepGoingFlightsRunner = new AtomicBoolean(true);
        draining = new AtomicBoolean(false);
        publishRunnerThread = new AtomicReference<>();
        flightsRunnerThread = new AtomicReference<>();

        publishRunnerDoneLatch = new CountDownLatch(1);
        flightsRunnerDoneLatch = new CountDownLatch(1);
    }

    /**
     * Start the publisher.
     */
    public void start() {
        Thread t = new Thread(this::publishRunner);
        t.start();
        publishRunnerThread.set(t);

        t = new Thread(this::flightsRunner);
        t.start();
        flightsRunnerThread.set(t);
    }

    /**
     * stop the publisher
     */
    public void stop() {
        keepGoingPublishRunner.set(false);
        keepGoingFlightsRunner.set(false);
    }

    /**
     * Drain the publish.
     * <p>The manager stops accepting new publishes</p>
     * <p>The manager tries to publish all already asked to be published</p>
     * You can still call stop, which will just finish it's current work.
     */
    public void drain() {
        draining.set(true);
        preFlight.offer(STOP_MARKER);
    }

    /**
     * Shutdown the manager.
     */
    @Override
    public void close() throws Exception {
        stop();

        if (notificationExecutorServiceWasNotSupplied) {
            notificationExecutorService.shutdown();
        }

        if (!publishRunnerDoneLatch.await(pollTime, TimeUnit.MILLISECONDS)) {
            Thread t = publishRunnerThread.get();
            if (t != null) {
                t.interrupt();
            }
        }

        if (!flightsRunnerDoneLatch.await(pollTime, TimeUnit.MILLISECONDS)) {
            Thread t = flightsRunnerThread.get();
            if (t != null) {
                t.interrupt();
            }
        }
    }

    /**
     * The number of messages currently in flight (published, awaiting ack)
     * @return the number
     */
    public int inFlightSize() {
        return inFlight.size();
    }

    /**
     * The number of messages currently waiting to be published
     * @return the number
     */
    public int preFlightSize() {
        return preFlight.size();
    }

    /**
     * A latch that finishes when then publish runner event loop is complete
     * @return the latch
     */
    public CountDownLatch getPublishRunnerDoneLatch() {
        return publishRunnerDoneLatch;
    }

    /**
     * A latch that finishes when then flights runner event loop is complete
     * @return the latch
     */
    public CountDownLatch getFlightsRunnerDoneLatch() {
        return flightsRunnerDoneLatch;
    }

    /**
     * The configured max in flight
     * @return the value
     */
    public int getMaxInFlight() {
        return maxInFlight;
    }

    /**
     * The configured refill allowed at
     * @return the value
     */
    public int getRefillAllowedAt() {
        return refillAllowedAt;
    }

    /**
     * The configured poll time
     * @return the time in millis
     */
    public long getPollTime() {
        return pollTime;
    }

    /**
     * The configured hold pause time
     * @return the time in millis
     */
    public long getHoldPauseTime() {
        return holdPauseTime;
    }

    /**
     * The configured wait timeout
     * @return the time in millis
     */
    public long getWaitTimeout() {
        return waitTimeout;
    }

    /**
     * The publishRunner is the runnable event loop that's job is to published messages that the user has queue up.
     */
    public void publishRunner() {
        try {
            while (keepGoingPublishRunner.get()) {
                if (notHolding.get()) {
                    PreFlight pre = preFlight.poll(pollTime, TimeUnit.MILLISECONDS);
                    if (pre != null) {
                        if (pre == STOP_MARKER) {
                            keepGoingPublishRunner.set(false);
                            return;
                        }

                        CompletableFuture<PublishAck> fpa;
                        if (retryConfig == null) {
                            fpa = js.publishAsync(pre.subject, pre.headers, pre.body, pre.options);
                        }
                        else {
                            fpa = PublishRetrier.publishAsync(retryConfig, js, pre.subject, pre.headers, pre.body, pre.options);
                        }
                        Flight flight = new Flight(fpa, pre);
                        inFlight.offer(flight);
                        pre.flightFuture.complete(flight);
                        notifyPublished(flight);
                        if (inFlight.size() >= maxInFlight) {
                            notHolding.set(false);
                        }
                    }
                }
                else {
                    //noinspection BusyWait
                    Thread.sleep(holdPauseTime);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            flightsRunnerDoneLatch.countDown();
        }
    }

    /**
     * The flightsRunner is the runnable event loop that's job is to track the published messages and their futures.
     */
    public void flightsRunner() {
        try {
            while (keepGoingFlightsRunner.get()) {
                Flight flight = inFlight.poll(pollTime, TimeUnit.MILLISECONDS);
                if (flight == null) {
                    if (draining.get() && preFlight.isEmpty() && inFlight.isEmpty()) {
                        keepGoingFlightsRunner.set(false);
                        return;
                    }
                }
                else {
                    if (flight.publishAckFuture.isDone()) {
                        if (flight.publishAckFuture.isCompletedExceptionally()) {
                            notifyCompletedExceptionally(flight);
                        }
                        else if (publishListener != null) {
                            notifyAcked(flight);
                        }
                    }
                    else if (System.currentTimeMillis() - flight.publishTime > waitTimeout) {
                        flight.publishAckFuture.completeExceptionally(new IOException("Timeout or no response waiting for publish acknowledgement."));
                        notifyTimeout(flight);
                    }
                    else {
                        inFlight.offer(flight); // put it back in the queue for later
                    }
                    // once the in flight empty out, we are allowed to publish again
                    if (inFlight.size() <= refillAllowedAt) {
                        notHolding.set(true);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            flightsRunnerDoneLatch.countDown();
        }
    }

    private void notifyPublished(Flight flight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.published(flight));
        }
    }

    private void notifyCompletedExceptionally(Flight flight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.completedExceptionally(flight));
        }
    }

    private void notifyAcked(Flight flight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.acked(flight));
        }
    }

    private void notifyTimeout(Flight flight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.timeout(flight));
        }
    }

    /**
     * Creates a builder for the AsyncJsPublisher
     * @return the builder
     */
    public static Builder builder(JetStream js) {
        return new Builder(js);
    }

    /**
     * The builder class for the AsyncJsPublisher
     */
    public static class Builder {
        JetStream js;
        String idPrefix = NUID.nextGlobal();
        int maxInFlight = DEFAULT_MAX_IN_FLIGHT;
        int refillAllowedAt = DEFAULT_REFILL_AMOUNT;
        RetryConfig retryConfig;
        AsyncJsPublishListener publishListener;
        long pollTime = DEFAULT_POLL_TIME;
        long holdPauseTime = DEFAULT_PAUSE_TIME;
        long waitTimeout = DEFAULT_WAIT_TIMEOUT;
        ExecutorService notificationExecutorService;

        public Builder(JetStream js) {
            if (js == null) {
                throw new IllegalArgumentException("JetStream context is required.");
            }
            this.js = js;
        }

        public Builder idPrefix(String idPrefix) {
            this.idPrefix = idPrefix;
            return this;
        }

        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        public Builder refillAllowedAt(int refillAllowedAt) {
            this.refillAllowedAt = refillAllowedAt;
            return this;
        }

        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        public Builder publishListener(AsyncJsPublishListener publishListener) {
            this.publishListener = publishListener;
            return this;
        }

        public Builder pollTime(long pollTime) {
            this.pollTime = pollTime;
            return this;
        }

        public Builder holdPauseTime(long holdPauseTime) {
            this.holdPauseTime = holdPauseTime;
            return this;
        }

        public Builder waitTimeout(long waitTimeout) {
            this.waitTimeout = waitTimeout;
            return this;
        }

        public Builder notificationExecutorService(ExecutorService notificationExecutorService) {
            this.notificationExecutorService = notificationExecutorService;
            return this;
        }

        /**
         * Builds a AsyncJsPublisher without starting it, for instance to delay its start or to use custom threads
         * @return AsyncJsPublisher instance
         */
        public AsyncJsPublisher build() {
            return new AsyncJsPublisher(this);
        }

        /**
         * Builds a AsyncJsPublisher.
         * @return AsyncJsPublisher instance
         */
        public AsyncJsPublisher start() {
            AsyncJsPublisher p = new AsyncJsPublisher(this);
            p.start();
            return p;
        }
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(String subject, Headers headers, byte[] body, PublishOptions options) {
        if (draining.get()) {
            throw new IllegalStateException("Cannot publish after drain");
        }

        PreFlight p = new PreFlight(idPrefix + "-" + messageIdGenerator.incrementAndGet(), subject, headers, body, options);
        preFlight.offer(p);
        return p.flightFuture;
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(String subject, byte[] body) {
        return publishAsync(subject, null, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(String subject, Headers headers, byte[] body) {
        return publishAsync(subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(String subject, byte[] body, PublishOptions options) {
        return publishAsync(subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param message the message to publish
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(Message message) {
        return publishAsync(message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public CompletableFuture<Flight> publishAsync(Message message, PublishOptions options) {
        return publishAsync(message.getSubject(), message.getHeaders(), message.getData(), options);
    }
}
