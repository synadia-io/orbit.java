// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.synadia.retrier.RetryConfig;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This is the main entry point. Build an instance of this class and
 * publish to it instead of to the JetStream context.
 */
public class AsyncJsPublisher implements AutoCloseable {
    public static final int DEFAULT_MAX_IN_FLIGHT = 50;
    public static final int DEFAULT_REFILL_AMOUNT = 0;
    public static final long DEFAULT_POLL_TIME = 100;
    public static final long DEFAULT_HOLD_PAUSE_TIME = 100;
    public static final long DEFAULT_WAIT_TIMEOUT = DEFAULT_MAX_IN_FLIGHT * DEFAULT_POLL_TIME;

    private static final PreFlight STOP_MARKER = new PreFlight("STOP", null, null, null, null);

    private final AtomicLong messageIdGenerator;
    private final JetStream js;
    private final Supplier<String> messageIdSupplier;
    private final String idPrefix;
    private final int maxInFlight;
    private final int refillAllowedAt;
    private final RetryConfig retryConfig;
    private final AsyncJsPublishListener publishListener;
    private final long pollTime;
    private final long holdPauseTime;
    private final long waitTimeout;
    private final boolean processAcksInOrder;
    private final LinkedBlockingQueue<PreFlight> preFlight;
    private final LinkedBlockingQueue<InFlight> inFlights;
    private final AtomicBoolean notInHoldingPattern;
    private final AtomicBoolean draining;
    private final AtomicBoolean keepGoingPublishRunner;
    private final AtomicBoolean keepGoingFlightsRunner;
    private final ExecutorService notificationExecutorService;
    private final boolean executorWasntUserSupplied;
    private final AtomicReference<Thread> publishRunnerThread;
    private final AtomicReference<Thread> flightsRunnerThread;
    private final CountDownLatch publishRunnerDoneLatch;
    private final CountDownLatch flightsRunnerDoneLatch;

    private AsyncJsPublisher(Builder b) {
        js = b.js;
        if (b.messageIdSupplier == null) {
            idPrefix = new NUID().nextSequence();
            messageIdGenerator = new AtomicLong(0);
            messageIdSupplier = () -> idPrefix + "-" + messageIdGenerator.incrementAndGet();
        }
        else {
            idPrefix = null;
            messageIdGenerator = null;
            messageIdSupplier = b.messageIdSupplier;
        }
        maxInFlight = b.maxInFlight;
        refillAllowedAt = b.refillAllowedAt;
        retryConfig = b.retryConfig;
        publishListener = b.publishListener;
        pollTime = b.pollTime;
        holdPauseTime = b.holdPauseTime;
        waitTimeout = b.waitTimeout;
        processAcksInOrder = b.processAcksInOrder;

        if (b.notificationExecutorService == null) {
            notificationExecutorService = Executors.newFixedThreadPool(1);
            executorWasntUserSupplied = true;
        }
        else {
            notificationExecutorService = b.notificationExecutorService;
            executorWasntUserSupplied = false;
        }

        preFlight = new LinkedBlockingQueue<>();
        inFlights = new LinkedBlockingQueue<>();
        notInHoldingPattern = new AtomicBoolean(true);
        draining = new AtomicBoolean(false);
        keepGoingPublishRunner = new AtomicBoolean(true);
        keepGoingFlightsRunner = new AtomicBoolean(true);
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
        preFlight.offer(STOP_MARKER);
        draining.set(true);
    }

    /**
     * shutdown the publisher.
     */
    @Override
    public void close() throws Exception {
        stop();

        if (executorWasntUserSupplied) {
            notificationExecutorService.shutdown();
        }

        if (!publishRunnerDoneLatch.await(pollTime, TimeUnit.MILLISECONDS)) {
            Thread t = publishRunnerThread.get();
            if (t != null && t.isAlive()) {
                t.interrupt();
            }
        }

        if (!flightsRunnerDoneLatch.await(pollTime, TimeUnit.MILLISECONDS)) {
            Thread t = flightsRunnerThread.get();
            if (t != null && t.isAlive()) {
                t.interrupt();
            }
        }
    }

    /**
     * The number of messages currently in flight (published, awaiting ack)
     * @return the number
     */
    public int inFlightSize() {
        return inFlights.size();
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

    public boolean getProcessAcksInOrder() {
        return processAcksInOrder;
    }

    /**
     * The publishRunner is the runnable event loop that's job is to published messages that the user has queue up.
     */
    public void publishRunner() {
        try {
            while (keepGoingPublishRunner.get()) {
                if (notInHoldingPattern.get()) {
                    PreFlight pre = preFlight.poll(pollTime, TimeUnit.MILLISECONDS);
                    if (pre != null) {
                        if (pre == STOP_MARKER) {
                            return;
                        }

                        // if there is a retry config, publish with retry else regular publish
                        CompletableFuture<PublishAck> fpa;
                        if (retryConfig == null) {
                            fpa = js.publishAsync(pre.subject, pre.headers, pre.body, pre.options);
                        }
                        else {
                            fpa = PublishRetrier.publishAsync(retryConfig, js, pre.subject, pre.headers, pre.body, pre.options);
                        }

                        // The publish is now in flight, put it in the in flights queue
                        // and complete the future that shows this was published
                        InFlight flight = new InFlight(fpa, pre);
                        inFlights.offer(flight);
                        pre.inFlightFuture.complete(flight);
                        notifyPublished(flight);

                        // if we've reached the max in flight, put publishing on hold
                        // this is reset by the flights runner when the condition is met
                        if (inFlights.size() >= maxInFlight) {
                            notInHoldingPattern.set(false);
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
            keepGoingPublishRunner.set(false);
            publishRunnerDoneLatch.countDown();
        }
    }

    /**
     * The flightsRunner is the runnable event loop that's job is to track the published messages and their futures.
     */
    public void flightsRunner() {
        try {
            while (keepGoingFlightsRunner.get()) {
                InFlight head = inFlights.poll(pollTime, TimeUnit.MILLISECONDS);
                if (head == null) {
                    // no inFlight? draining? no more queued? no more in inFlight? we are done!
                    if (draining.get() && preFlight.isEmpty() && inFlights.isEmpty()) {
                        return;
                    }
                }
                else {
                    try {
                        // Turns out processing in order is faster
                        // I think the reason behind this is that while waiting for the one
                        // at the head of the queue, plus the processing time,
                        // allows more acks to complete. It's like head of line blocking but in a good way.
                        PublishAck pa = head.publishAckFuture.get(waitTimeout, TimeUnit.MILLISECONDS);
                        notifyCompleted(new PostFlight(head, pa));
                    }
                    catch (ExecutionException e) {
                        handleExecutionException(e, head);
                    }
                    catch (TimeoutException e) {
                        notifyTimeout(new PostFlight(head, true, false, e));
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    // once the inFlight empty out/cross the refill threshold,
                    // we are allowed to publish again
                    if (inFlights.size() <= refillAllowedAt) {
                        notInHoldingPattern.set(true);
                    }
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            keepGoingFlightsRunner.set(false);
            flightsRunnerDoneLatch.countDown();
        }
    }

    private void handleExecutionException(ExecutionException e, InFlight inFlight) {
        Throwable cause = e.getCause() == null ? e : e.getCause();
        if (cause instanceof JetStreamApiException) {
            if (cause.getMessage().contains("10060") // expected stream does not match [10060]
                || (cause.getMessage().contains("10070")) // wrong last msg ID: [10070]
                || (cause.getMessage().contains("10071")) // wrong last sequence: n [10071]
            )
            {
                notifyCompletedExceptionally(new PostFlight(inFlight, false, true, cause));
            }
            else {
                notifyCompletedExceptionally(new PostFlight(inFlight, cause));
            }
        }
        else if (cause instanceof IOException) {
            if (cause.getMessage().contains("Timeout or no response")) {
                notifyTimeout(new PostFlight(inFlight, true, false, cause));
            }
            else {
                notifyCompletedExceptionally(new PostFlight(inFlight, cause));
            }
        }
    }

    private void notifyPublished(InFlight inFlight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.published(inFlight));
        }
    }

    private void notifyCompletedExceptionally(PostFlight postFlight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.completedExceptionally(postFlight));
        }
    }

    private void notifyCompleted(PostFlight postFlight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.acked(postFlight));
        }
    }

    private void notifyTimeout(PostFlight postFlight) {
        if (publishListener != null) {
            notificationExecutorService.submit(() -> publishListener.timeout(postFlight));
        }
    }

    /**
     * Creates a builder for the AsyncJsPublisher
     * @param js the JetStream context
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
        Supplier<String> messageIdSupplier;
        int maxInFlight = DEFAULT_MAX_IN_FLIGHT;
        int refillAllowedAt = DEFAULT_REFILL_AMOUNT;
        RetryConfig retryConfig;
        AsyncJsPublishListener publishListener;
        long pollTime = DEFAULT_POLL_TIME;
        long holdPauseTime = DEFAULT_HOLD_PAUSE_TIME;
        long waitTimeout = DEFAULT_WAIT_TIMEOUT;
        boolean processAcksInOrder = true;
        ExecutorService notificationExecutorService;

        public Builder(JetStream js) {
            if (js == null) {
                throw new IllegalArgumentException("JetStream context is required.");
            }
            this.js = js;
        }

        /**
         * Provide a supplier function that generates an internal message id.
         * If the message has publish options and there is a message id, this function is
         * ignored and the publish options message id is used.
         * @param messageIdSupplier the supplier
         * @return the builder
         */
        public Builder messageIdSupplier(Supplier<String> messageIdSupplier) {
            this.messageIdSupplier = messageIdSupplier;
            return this;
        }

        /**
         * The maximum number of messages that can be in flight.
         * Defaults to  {@value #DEFAULT_MAX_IN_FLIGHT}
         * In flight is defined as a message that has been published but not yet
         * completed either with a publish ack confirmation or an exception.
         * Once the in flight maximum has been reached, no messages will be
         * published until the number of messages in flight becomes less than
         * or equal to the refill allowed at
         * @param maxInFlight the maximum number of messages in flight
         * @return the builder
         */
        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        /**
         * The number of messages to allow starting to refill the in flight queue
         * if the in flight queue had reached the max in flight
         * Defaults to 0 meaning it must be completely empty once it gets full
         * @param refillAllowedAt the amount
         * @return the builder
         */
        public Builder refillAllowedAt(int refillAllowedAt) {
            this.refillAllowedAt = refillAllowedAt;
            return this;
        }

        /**
         * If a retry config is supplied, the publish will be done with the Retrier
         * using the supplied config. If no retry config is supplied, the publish
         * is just a standard one time publish
         * @param retryConfig the config
         * @return the buidler
         */
        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        /**
         * The publish listener is a callback interface that allows
         * the developer to monitor and control the publishing
         * @param publishListener the listener
         * @return the builder
         */
        public Builder publishListener(AsyncJsPublishListener publishListener) {
            this.publishListener = publishListener;
            return this;
        }

        /**
         * The amount of time to poll a queue
         * Defaults to {@value #DEFAULT_POLL_TIME}
         * @param pollTime the time in milliseconds
         * @return the builder
         */
        public Builder pollTime(long pollTime) {
            this.pollTime = pollTime;
            return this;
        }

        /**
         * The amount of time to pause if the publish loop is in the holding pattern
         * The holding pattern happens once the in flight queue is filled to the max in flight
         * and the completed acks have not cleared the refill at amount.
         * Defaults to {@value #DEFAULT_HOLD_PAUSE_TIME}
         * @param holdPauseTime the time in milliseconds
         * @return the builder
         */
        public Builder holdPauseTime(long holdPauseTime) {
            this.holdPauseTime = holdPauseTime;
            return this;
        }

        /**
         * The amount of time to poll the publish ack future to see if it's done.
         * Defaults to {@value #DEFAULT_WAIT_TIMEOUT}
         * @param waitTimeout the time in milliseconds
         * @return the builder
         */
        public Builder waitTimeout(long waitTimeout) {
            this.waitTimeout = waitTimeout;
            return this;
        }

        /**
         * Defaults to true. Important if there are publish expectations on a message
         * otherwise can be set to false. Will cause publish ack futures to be processed
         * in the smae order they were published/queued
         * @param processAcksInOrder the setting
         * @return the builder
         * @deprecated Turns out processing in order is faster,
         *             my guess is that by waiting for the get, acks at the end of the queue complete.
         */
        @Deprecated
        public Builder processAcksInOrder(boolean processAcksInOrder) {
            this.processAcksInOrder = processAcksInOrder;
            return this;
        }

        /**
         * The publish listener receives notifications as an executor task.
         * The developer can provide their own service, otherwise one will be provided
         * @param notificationExecutorService the service
         * @return the builder
         */
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
         * Builds a AsyncJsPublisher and starts using the built-in threads.
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
    public PreFlight publishAsync(String subject, Headers headers, byte[] body, PublishOptions options) {
        if (draining.get() || (!keepGoingPublishRunner.get() && !keepGoingFlightsRunner.get())) {
            throw new IllegalStateException("Cannot publish once drained or stopped.");
        }

        String messageId = options != null && options.getMessageId() != null
            ? options.getMessageId() : messageIdSupplier.get();
        PreFlight p = new PreFlight(messageId, subject, headers, body, options);
        preFlight.offer(p);
        return p;
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public PreFlight publishAsync(String subject, byte[] body) {
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
    public PreFlight publishAsync(String subject, Headers headers, byte[] body) {
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
    public PreFlight publishAsync(String subject, byte[] body, PublishOptions options) {
        return publishAsync(subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param message the message to publish
     * @return The future
     */
    public PreFlight publishAsync(Message message) {
        return publishAsync(message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public PreFlight publishAsync(Message message, PublishOptions options) {
        return publishAsync(message.getSubject(), message.getHeaders(), message.getData(), options);
    }
}
