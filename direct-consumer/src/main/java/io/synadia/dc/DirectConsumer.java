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
    final AtomicLong lastSeq;
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
        lastSeq = new AtomicLong(b.startSequence == null || b.startSequence < 2 ? 0 : b.startSequence - 1);
        numPending = new AtomicLong(-1);

        statusesInARow = new AtomicInteger();
        inFlight = new AtomicInteger();
        requestDelay = new AtomicLong();
        stopConsuming = true;
        working = false;
    }

    public MessageInfo next() {
        start();
        AtomicReference<MessageInfo> ref = new AtomicReference<>();
        requestBatch(1, ref::set);
        return ref.get();
    }

    public List<MessageInfo> fetch() {
        start();
        List<MessageInfo> list = new ArrayList<>();
        requestBatch(batch, list::add);
        return list;
    }

    public CompletableFuture<Boolean> consume(ExecutorService executor, MessageInfoHandler handler) {
        start();
        return CompletableFuture.supplyAsync(() -> _consume(handler), executor);
    }

    public LinkedBlockingQueue<MessageInfo> queueConsume(ExecutorService executor) {
        start();
        final LinkedBlockingQueue<MessageInfo> q = new LinkedBlockingQueue<>();
        consume(executor, q::add);
        return q;
    }

    private void start() {
        if (working) {
            throw new IllegalStateException(ERROR_ONE_AT_A_TIME);
        }
        stopConsuming = false;
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
        while (!stopConsuming) {
            if (requestDelay.get() > 0) {
                try {
                    //noinspection BusyWait
                    Thread.sleep(requestDelay.get());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopConsuming();
                    return false;
                }
            }
            requestBatch(batch, handler);
        }
        return true;
    }

    private boolean requestBatch(int batch, MessageInfoHandler handler) {
        try {
            working = true;
            resetDelay();

            MessageBatchGetRequest mbgr;
            if (startTime.get() == null) {
                mbgr = MessageBatchGetRequest.batch(subject, batch, lastSeq.get() + 1);
            }
            else {
                mbgr = MessageBatchGetRequest.batch(subject, batch, startTime.get());
                startTime.set(null); // all subsequent requests will start at last seq
            }

            return jsm.requestMessageBatch(stream, mbgr, mi -> {
                if (mi.isMessage()) {
                    handler.onMessageInfo(mi);
                    lastSeq.set(mi.getSeq());
                    numPending.set(mi.getNumPending());
                    statusesInARow.set(0);
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
            return false;
        }
        finally {
            working = false;
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
