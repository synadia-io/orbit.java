// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.dc;

import io.nats.client.JetStreamManagement;
import io.nats.client.MessageInfoHandler;
import io.nats.client.api.MessageBatchGetRequest;
import io.nats.client.api.MessageInfo;
import io.nats.client.support.Status;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DirectConsumer {

    public static final String ERROR_ONE_AT_A_TIME = "Only allowed one request at a time.";
    final JetStreamManagement jsm;
    final String stream;
    final String subject;
    final int batch;
    final AtomicReference<ZonedDateTime> startTime;

    final long[] backoffPolicy;
    final int backoffPolicies;

    final LinkedBlockingQueue<MessageInfo> queue;
    final AtomicLong expectedLastSequence;
    final AtomicLong nextStartSequence;
    final AtomicLong numPending;
    final AtomicInteger statusesInARow;
    final AtomicInteger inFlight;
    final AtomicLong requestDelay;

    volatile boolean stopConsuming;
    volatile boolean working;

    DirectConsumer(DirectConsumerBuilder b) {
        this.jsm = b.jsm;
        this.stream = b.stream;
        this.subject = b.subject;
        this.batch = b.batch;
        this.startTime = new AtomicReference<>(b.startTime);
        this.backoffPolicy = b.backoffPolicy;
        backoffPolicies = b.backoffPolicy.length;

        queue = new LinkedBlockingQueue<>();
        expectedLastSequence = new AtomicLong(0);
        nextStartSequence = new AtomicLong(b.startSequence == null || b.startSequence < 2 ? 0 : b.startSequence);
        numPending = new AtomicLong(-1);

        statusesInARow = new AtomicInteger();
        inFlight = new AtomicInteger();
        requestDelay = new AtomicLong();
        stopConsuming = true;
        working = false;
    }

    public MessageInfo next() {
        startWorking();
        AtomicReference<MessageInfo> ref = new AtomicReference<>();
        requestBatch(1, true, ref::set);
        return ref.get();
    }

    public List<MessageInfo> fetch() {
        startWorking();
        List<MessageInfo> list = new ArrayList<>();
        requestBatch(batch, true, list::add);
        return list;
    }

    public CompletableFuture<Boolean> consume(ExecutorService executor, MessageInfoHandler handler) {
        startWorking();
        return CompletableFuture.supplyAsync(() -> _consume(handler), executor);
    }

    public LinkedBlockingQueue<MessageInfo> queueConsume(ExecutorService executor) {
        startWorking();
        final LinkedBlockingQueue<MessageInfo> q = new LinkedBlockingQueue<>();
        consume(executor, q::add);
        return q;
    }

    private void startWorking() {
        if (working) {
            throw new IllegalStateException(ERROR_ONE_AT_A_TIME);
        }
        working = true;
    }

    public void stopConsuming() {
        stopConsuming = true;
    }

    public boolean isStopped() {
        return stopConsuming;
    }

    public boolean isWorking() {
        return working;
    }

    private boolean _consume(MessageInfoHandler handler) {
        stopConsuming = false;
        while (!stopConsuming) {
            if (requestDelay.get() > 0) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(requestDelay.get());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopConsuming();
                    working = false;
                    return false;
                }
            }
            requestBatch(batch, false, handler);
        }
        working = false;
        return true;
    }

    private void requestBatch(int batch, boolean clearWorking, MessageInfoHandler handler) {
        try {
            working = true;
            resetDelay();

            MessageBatchGetRequest mbgr;
            if (startTime.get() == null) {
                mbgr = MessageBatchGetRequest.batch(subject, batch, nextStartSequence.get());
            }
            else {
                mbgr = MessageBatchGetRequest.batch(subject, batch, startTime.get());
                startTime.set(null); // all subsequent requests will start at last seq
            }
            expectedLastSequence.set(0);

            jsm.requestMessageBatch(stream, mbgr, mi -> {
                if (mi.isMessage()) {
                    System.out.println(mi);
//                    if (expectedLastSequence.get() != mi.getLastSeq()) {
//                        throw new Exception("Unexpected messages sequence received.");
//                    }
                    expectedLastSequence.set(mi.getSeq());
                    nextStartSequence.set(mi.getSeq() + 1);
                    numPending.set(mi.getNumPending());
                    statusesInARow.set(0);
                    handler.onMessageInfo(mi);
                    resetDelay();
                }
                else {
                    statusesInARow.incrementAndGet();
                    int code = mi.getStatus().getCode();
                    if (code == 204 || code == 404) {
                        nextDelay();
                    }
                    else {
                        handler.onMessageInfo(mi);
                        setMaxDelay();
                    }
                }
            });
        }
        catch (Exception e) {
            MessageInfo mi = new MessageInfo(new Status(400, e.getMessage()), stream, true);
            handler.onMessageInfo(mi);
        }
        finally {
            if (clearWorking) {
                working = false;
            }
        }
    }

    private void resetDelay() {
        requestDelay.set(0);
    }

    private void nextDelay() {
        requestDelay.set(backoffPolicy[(statusesInARow.get() - 1) % backoffPolicies]);
    }

    private void setMaxDelay() {
        requestDelay.set(backoffPolicy[backoffPolicies - 1]);
    }
}
