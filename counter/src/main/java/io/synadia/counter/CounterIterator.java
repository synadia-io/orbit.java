// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counter;

import io.nats.client.api.MessageInfo;
import io.synadia.direct.MessageInfoHandler;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CounterIterator implements Iterator<BigInteger>, MessageInfoHandler {
    private final LinkedBlockingQueue<BigInteger> queue;
    private final AtomicBoolean isFinished;
    private BigInteger nextElement;
    private boolean hasNextComputed;

    public CounterIterator() {
        this.queue = new LinkedBlockingQueue<>();
        this.isFinished = new AtomicBoolean(false);
        this.nextElement = null;
        this.hasNextComputed = false;
    }

    @Override
    public void onMessageInfo(MessageInfo messageInfo) {
//        if (endMarker.equals(value)) {
//            isFinished.set(true);
//        } else {
//            try {
//                queue.put(value);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                throw new RuntimeException("Interrupted while adding value", e);
//            }
//        }
    }

    @Override
    public boolean hasNext() {
        if (!hasNextComputed) {
            computeNext();
        }
        return nextElement != null;
    }

    @Override
    public BigInteger next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }
        BigInteger result = nextElement;
        hasNextComputed = false;
        nextElement = null;
        return result;
    }

    private void computeNext() {
        if (hasNextComputed) {
            return;
        }

        try {
            // If we've already seen the end marker, no more elements
            if (isFinished.get() && queue.isEmpty()) {
                nextElement = null;
            } else {
                // Wait for next element with timeout to avoid infinite blocking
                nextElement = queue.poll(100, TimeUnit.MILLISECONDS);

                // If queue is empty but not finished, keep polling
                while (nextElement == null && !isFinished.get()) {
                    nextElement = queue.poll(100, TimeUnit.MILLISECONDS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for next element", e);
        }

        hasNextComputed = true;
    }
}
