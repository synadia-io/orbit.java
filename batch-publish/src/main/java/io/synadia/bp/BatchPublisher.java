// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsJetStreamConstants;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.PublishOptions.DEFAULT_TIMEOUT;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Validator.*;

public class BatchPublisher {
    enum State {
        Open, Closed, Discarded
    }

    private final String batchId;
    private final Connection conn;
    private final Duration ackTimeout;
    private final boolean ackFirst;
    private final int ackEvery;
    private final MessageTtl messageTtl;

    private final Headers headers; // final to be re-used/cleared
    private int lastSeq;
    private State state;

    private BatchPublisher(BatchPublisher.Builder b) {
        batchId = b.batchId;
        conn = b.conn;
        ackTimeout = b.ackTimeout;
        ackFirst = b.ackFirst;
        ackEvery = b.ackEvery;
        messageTtl = b.messageTtl;

        headers = new Headers();
        lastSeq = 0;
        state = State.Open;
    }

    @Nullable
    public String getBatchId() {
        return batchId;
    }

    @NonNull
    public Duration getAckTimeout() {
        return ackTimeout;
    }

    public boolean ackFirst() {
        return ackFirst;
    }

    public int getAckEvery() {
        return ackEvery;
    }

    /**
     * Gets the message ttl string. Might be null. Might be "never".
     * 10 seconds would be "10s" for the server
     * @return the message ttl string
     */
    public String getMessageTtl() {
        return messageTtl == null ? null : messageTtl.getTtlString();
    }

    public int size() {
        return lastSeq;
    }

    public void discard() {
        state = State.Discarded;
    }

    public boolean isOpen() {
        return state == State.Open;
    }

    public boolean isDiscarded() {
        return state == State.Discarded;
    }

    public boolean isClosed() {
        return state == State.Closed;
    }

    public void add(String subject, byte[] data) throws BatchPublishException {
        add(subject, null, data, null);
    }

    public void add(String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        add(subject, null, data, opts);
    }

    public void add(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        add(subject, userHeaders, data, null);
    }

    public void add(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        if (state != State.Open) {
            throw new BatchPublishException(batchId, "Batch not open: " + state);
        }
        if (   (++lastSeq == 1 && ackFirst)               // first publish
            || (ackEvery > 0 && lastSeq % ackEvery == 0)) // or every publish
        {
            _addAcked(subject, userHeaders, data, opts);
        }
        else {
            updateHeaders(false, userHeaders, opts);
            conn.publish(subject, headers, data);
        }
    }

    public void addAcked(String subject, byte[] data) throws BatchPublishException {
        addAcked(subject, null, data, null);
    }

    public void addAcked(String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        addAcked(subject, null, data, opts);
    }

    public void addAcked(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        addAcked(subject, userHeaders, data, null);
    }

    public void addAcked(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        if (state != State.Open) {
            throw new BatchPublishException(batchId, "Batch not open: " + state);
        }
        ++lastSeq;
        _addAcked(subject, userHeaders, data, opts);
    }

    private void _addAcked(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        Message m = request(subject, userHeaders, data, false, opts);
        if (m.getData().length != 0) {
            throw new BatchPublishException(batchId, "Invalid ack returned from add with confirm");
        }
    }

    public PublishAck commit(String subject, byte[] data) throws BatchPublishException {
        return commit(subject, null, data, null);
    }

    public PublishAck commit(String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        return commit(subject, null, data, opts);
    }

    public PublishAck commit(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        return commit(subject, userHeaders, data, null);
    }

    public PublishAck commit(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        if (state != State.Open) {
            throw new BatchPublishException(batchId, "Batch not open: " + state);
        }
        try {
            ++lastSeq;
            Message m = request(subject, userHeaders, data, true, opts);
            return new PublishAck(m);
        }
        catch (IOException e) {
            // done this way because PublishAck makes an IOException if the ack is invalid.
            // it was done that way because of api backward compatibility
            // just no need of the extra layer
            throw new BatchPublishException(batchId, e.getMessage());
        }
        catch (JetStreamApiException e) {
            throw new BatchPublishException(batchId, e);
        }
        finally {
            state = State.Closed;
        }
    }

    public CompletableFuture<PublishAck> commitAsync(String subject, byte[] data) {
        return commitAsync(subject, null, data, null);
    }

    public CompletableFuture<PublishAck> commitAsync(String subject, byte[] data, BatchPublishOptions opts) {
        return commitAsync(subject, null, data, opts);
    }

    public CompletableFuture<PublishAck> commitAsync(String subject, Headers userHeaders, byte[] data) {
        return commitAsync(subject, userHeaders, data, null);
    }

    public CompletableFuture<PublishAck> commitAsync(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return commit(subject, userHeaders, data, opts);
            }
            catch (BatchPublishException e) {
                throw new RuntimeException(e);
            }
        }, conn.getOptions().getExecutor());
    }

    private Message request(String subject, Headers userHeaders, byte[] data, boolean commit, BatchPublishOptions opts) throws BatchPublishException {
        try {
            updateHeaders(commit, userHeaders, opts);
            CompletableFuture<Message> f = conn.requestWithTimeout(subject, headers, data, ackTimeout);
            return f.get(ackTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new BatchPublishException(batchId, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchPublishException(batchId, e);
        }
    }

    private void updateHeaders(boolean commit, Headers userHeaders, BatchPublishOptions bpOpts) {
        headers.clear();
        headers.put(NATS_BATCH_ID_HDR, batchId);
        headers.put(NatsJetStreamConstants.NATS_BATCH_SEQUENCE_HDR, Integer.toString(lastSeq));

        if (commit) {
            headers.put(NatsJetStreamConstants.NATS_BATCH_COMMIT_HDR, "1");
        }

        if (userHeaders != null && !userHeaders.isEmpty()) {
            Set<String> keys = userHeaders.keySet();
            for (String key : keys) {
                headers.put(key, userHeaders.get(key));
            }
        }

        if (bpOpts != null) {
            long value = bpOpts.getExpectedLastSequence();
            if (value > -1) {
                headers.put(EXPECTED_LAST_SEQ_HDR, Long.toString(value));
            }
            value = bpOpts.getExpectedLastSubjectSequence();
            if (value > -1) {
                headers.put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(value));
            }
            String temp = bpOpts.getExpectedLastSubjectSequenceSubject();
            if (temp != null) {
                headers.put(EXPECTED_LAST_SUB_SEQ_SUB_HDR, temp);
            }
            temp = bpOpts.getExpectedStream();
            if (temp != null) {
                headers.put(EXPECTED_STREAM_HDR, temp);
            }

            // message ttl can come from the BatchPublishOptions first
            // then can come from the BatchPublisher second
            temp = bpOpts.getMessageTtl();
            if (temp == null) {
                temp = messageTtl == null ? null : messageTtl.getTtlString();
            }
            if (temp != null) {
                headers.put(MSG_TTL_HDR, temp);
            }
        }
    }

    /**
     * Get an instance of the builder, same as new BatchPublisher.Builder();
     * @return The Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the BatchPublisher
     */
    public static class Builder {
        private Connection conn;
        private Duration ackTimeout;
        private String batchId;
        private boolean ackFirst = true;
        private int ackEvery;
        private MessageTtl messageTtl;

        public Builder connection(Connection conn) {
            this.conn = conn;
            return this;
        }

        public Builder batchId(String batchId) {
            this.batchId = batchId;
            return this;
        }

        /**
         * Sets the timeout to wait for the acknowledgement for acks when adding or the commit.
         * @param ackTimeout the ack timeout.
         * @return The Builder
         */
        public Builder ackTimeout(Duration ackTimeout) {
            this.ackTimeout = validateDurationNotRequiredGtOrEqZero(ackTimeout, DEFAULT_TIMEOUT);
            return this;
        }

        /**
         * Sets the timeout im milliseconds to wait for the acknowledgement for acks when adding or the commit.
         * @param ackTimeoutMillis the ack timeout.
         * @return The Builder
         */
        public Builder ackTimeout(long ackTimeoutMillis) {
            this.ackTimeout = ackTimeoutMillis < 1 ? DEFAULT_TIMEOUT : Duration.ofMillis(ackTimeoutMillis);
            return this;
        }

        /**
         * Whether to ack the first message. Defaults to true
         * @param ackFirst the flag
         * @return The Builder
         */
        public Builder ackFirst(boolean ackFirst) {
            this.ackFirst = ackFirst;
            return this;
        }

        /**
         * The interval to ack when adding a message, after the first message. Defaults to 0 (never).
         * @param ackEvery the ack every value
         * @return The Builder
         */
        public Builder ackEvery(int ackEvery) {
            this.ackEvery = ackEvery < 1 ? 0 : ackEvery;
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published.
         * Less than 1 has the effect of clearing the message ttl
         * @param msgTtlSeconds the ttl in seconds
         * @return The Builder
         */
        public Builder messageTtlSeconds(int msgTtlSeconds) {
            this.messageTtl = msgTtlSeconds < 1 ? null : MessageTtl.seconds(msgTtlSeconds);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published. Use at your own risk.
         * The current specification can be found here @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
         * Null or empty has the effect of clearing the message ttl
         * @param msgTtlCustom the custom ttl string
         * @return The Builder
         */
        public Builder messageTtlCustom(String msgTtlCustom) {
            this.messageTtl = nullOrEmpty(msgTtlCustom) ? null : MessageTtl.custom(msgTtlCustom);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published and never be expired
         * @return The Builder
         */
        public Builder messageTtlNever() {
            this.messageTtl = MessageTtl.never();
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published
         * @param messageTtl the message ttl instance
         * @return The Builder
         */
        public Builder messageTtl(MessageTtl messageTtl) {
            this.messageTtl = messageTtl;
            return this;
        }

        public BatchPublisher build() {
            validateNotNull(conn, "Connection required,");
            if (!conn.getServerInfo().isNewerVersionThan("2.11.99")) {
                throw new IllegalArgumentException("Batch direct get not available until server version 2.11.0.");
            }
            if (ackTimeout == null) {
                ackTimeout = conn.getOptions().getConnectionTimeout();
            }
            batchId = emptyAsNull(batchId);
            if (batchId == null) {
                batchId = new NUID().next();
            }
            else if (batchId.length() > 64){
                throw new IllegalArgumentException("Batch ID cannot be longer than 64 characters");
            }
            return new BatchPublisher(this);
        }
    }
}
