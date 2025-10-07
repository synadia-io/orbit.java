// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counters;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CounterIterator implements Iterator<CounterEntryResponse> {
    private final LinkedBlockingQueue<CounterEntryResponse> queue;
    private final Duration timeoutFirst;
    private final Duration timeoutSubsequent;
    private CounterEntryResponse nextElement;
    private boolean first;
    private boolean terminated;

    public CounterIterator(LinkedBlockingQueue<CounterEntryResponse> queue, Duration timeout) {
        this(queue, timeout, timeout);
    }

    public CounterIterator(LinkedBlockingQueue<CounterEntryResponse> queue, Duration timeoutFirst, Duration timeoutSubsequent) {
        this.queue = queue;
        this.timeoutFirst = timeoutFirst;
        this.timeoutSubsequent = timeoutSubsequent;
        first = true;
        terminated = false;
    }

    @Override
    public boolean hasNext() {
        try {
            if (terminated) {
                return false;
            }
            if (nextElement == null) {
                nextElement = queue.poll(first ? timeoutFirst.toNanos() : timeoutSubsequent.toNanos(), TimeUnit.NANOSECONDS);
                first = false;
                if (nextElement != null && !nextElement.isEntry()) {
                    nextElement = null;
                    terminated = true;
                }
            }
            return nextElement != null;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public CounterEntryResponse next() {
        CounterEntryResponse current = nextElement;
        nextElement = null;
        return current;
    }
}
