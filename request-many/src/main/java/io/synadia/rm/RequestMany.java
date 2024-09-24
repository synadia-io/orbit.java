// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.rm;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.EMPTY_BODY;

/**
 * The RequestMany is
 */
public class RequestMany {
    public static final long DEFAULT_TOTAL_WAIT_TIME_MS = 1000;
    private static final int MIN_INTERVAL = 10;

    private final Connection conn;
    private final Supplier<String> inboxSupplier;
    private final long totalWaitTime;
    private final long maxStall;
    private final long maxResponses;

    public RequestMany(Builder b) {
        this.conn = b.conn;
        this.inboxSupplier = b.inboxSupplier == null ? conn::createInbox : b.inboxSupplier;
        this.totalWaitTime = b.totalWaitTime;
        this.maxStall = b.maxStall;
        this.maxResponses = b.maxResponses;
    }

    public static Builder builder(Connection conn) {
        return new Builder(conn);
    }

    public static class Builder {
        private final Connection conn;
        private Supplier<String> inboxSupplier;
        private long totalWaitTime = DEFAULT_TOTAL_WAIT_TIME_MS;
        private long maxStall = Long.MAX_VALUE;
        private long maxResponses = Long.MAX_VALUE;

        public Builder(Connection conn) {
            this.conn = conn;
        }

        public Builder inboxSupplier(Supplier<String> inboxSupplier) {
            this.inboxSupplier = inboxSupplier;
            return this;
        }

        public Builder totalWaitTime(long totalWaitTime) {
            this.totalWaitTime = totalWaitTime;
            return this;
        }

        public Builder maxStall(long maxStall) {
            this.maxStall = maxStall;
            return this;
        }

        public Builder maxResponses(long maxResponses) {
            this.maxResponses = maxResponses;
            return this;
        }

        public RequestMany build() {
            return new RequestMany(this);
        }
    }

    public List<Message> fetch(String subject, byte[] payload) {
        return fetch(subject, null, payload);
    }

    public List<Message> fetch(String subject, Headers headers, byte[] payload) {
        List<Message> results = new ArrayList<>();
        consume(subject, headers, payload, msg -> {
            if (msg != EOD) {
                results.add(msg);
            }
            return true;
        });
        return results;
    }

    @SuppressWarnings("DataFlowIssue")
    public static final Message EOD = new NatsMessage("EOD", null, EMPTY_BODY);

    public LinkedBlockingQueue<Message> iterate(String subject, byte[] payload) {
        return iterate(subject, null, payload);
    }

    public LinkedBlockingQueue<Message> iterate(String subject, Headers headers, byte[] payload) {
        final LinkedBlockingQueue<Message> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(() -> {
            consume(subject, headers, payload, msg -> {
                q.add(msg);
                return true;
            });
        });
        return q;
    }

    public void consume(String subject, byte[] payload, RmConsumer consumer) {
        consume(subject, null, payload, consumer);
    }

    public void consume(String subject, Headers headers, byte[] payload, RmConsumer rmc) {
        Subscription sub = null;
        try {
            String replyTo = inboxSupplier.get();
            sub = conn.subscribe(replyTo);
            conn.publish(subject, replyTo, headers, payload);

            long resultsLeft = maxResponses;
            long start = System.currentTimeMillis();
            long timeLeft = totalWaitTime;
            long timeout = timeLeft; // first time we wait the whole timeout
            while (timeLeft > 0) {
                Message msg = sub.nextMessage(timeout);
                timeLeft = totalWaitTime - (System.currentTimeMillis() - start);
                timeout = Math.min(timeLeft, maxStall); // subsequent times we wait the shortest of the time left vs the max interval

                if (msg == null || !rmc.consume(msg) || --resultsLeft < 1) {
                    return;
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            rmc.consume(EOD);
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            }
            catch (Exception ignore) {}
        }
    }
}
