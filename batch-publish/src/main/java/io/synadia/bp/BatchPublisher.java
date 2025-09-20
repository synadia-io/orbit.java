// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.NUID;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsJetStreamConstants;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.support.Validator.emptyAsNull;
import static io.nats.client.support.Validator.validateNotNull;

public class BatchPublisher {
    enum State {
        Open, Closed, Discarded
    }

    private final Connection conn;
    private final Duration requestTimeout;
    private final Headers headers;
    private final List<String> lastUserHeaderKeys;
    private final String batchId;

    private int lastSeq;
    private State state;


    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the BatchPublisher
     */
    public static class Builder {
        private Connection conn;
        private Duration requestTimeout;
        private String batchId;

        public Builder connection(Connection conn) {
            this.conn = conn;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder batchId(String batchId) {
            this.batchId = batchId;
            return this;
        }

        public BatchPublisher build() {
            validateNotNull(conn, "Connection required,");
            if (!conn.getServerInfo().isNewerVersionThan("2.11.99")) {
                throw new IllegalArgumentException("Batch direct get not available until server version 2.11.0.");
            }
            if (requestTimeout == null) {
                requestTimeout = conn.getOptions().getConnectionTimeout();
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

    private BatchPublisher(BatchPublisher.Builder b) {
        conn = b.conn;
        requestTimeout = b.requestTimeout;
        batchId = b.batchId;
        headers = new Headers();
        headers.put(NatsJetStreamConstants.NATS_BATCH_ID_HDR, batchId);
        lastUserHeaderKeys = new ArrayList<>();
        lastSeq = 0;
        state = State.Open;
    }

    @NonNull
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    @Nullable
    public String getBatchId() {
        return batchId;
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
        add(subject, null, data);
    }

    public void add(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        if (state != State.Open) {
            throw new IllegalStateException("Batch not open: " + state);
        }
        if (lastSeq == 0) { // first publish
            request(subject, userHeaders, data, false);
        }
        else {
            updateHeaders(userHeaders, false);
            conn.publish(subject, headers, data);
        }
    }

    public void addWithConfirm(String subject, byte[] data) throws BatchPublishException {
        addWithConfirm(subject, null, data);
    }

    public void addWithConfirm(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        if (state != State.Open) {
            throw new IllegalStateException("Batch not open: " + state);
        }
        request(subject, userHeaders, data, false);
    }

    public PublishAck commit(String subject, byte[] data) throws BatchPublishException {
        return commit(subject, null, data);
    }

    public PublishAck commit(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        try {
            return new PublishAck(request(subject, userHeaders, data, true));
        }
        catch (JetStreamApiException | IOException e) {
            throw new BatchPublishException(e);
        }
        finally {
            state = State.Closed;
        }
    }

    private Message request(String subject, Headers userHeaders, byte[] data, boolean commit) throws BatchPublishException {
        if (headers == null) {
            throw new IllegalStateException("Batch not opened");
        }
        try {
            updateHeaders(userHeaders, commit);
            CompletableFuture<Message> f = conn.requestWithTimeout(subject, headers, data, requestTimeout);
            return f.get(requestTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new BatchPublishException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchPublishException(e);
        }
    }

    private void updateHeaders(Headers userHeaders, boolean commit) {
        if (!lastUserHeaderKeys.isEmpty()) {
            headers.remove(lastUserHeaderKeys);
            lastUserHeaderKeys.clear();
        }
        if (userHeaders != null && !userHeaders.isEmpty()) {
            Set<String> keys = userHeaders.keySet();
            for (String key : keys) {
                headers.add(key, userHeaders.get(key));
                lastUserHeaderKeys.add(key);
            }
        }
        headers.put(NatsJetStreamConstants.NATS_BATCH_SEQUENCE_HDR, Integer.toString(++lastSeq));
        if (commit) {
            headers.put(NatsJetStreamConstants.NATS_BATCH_COMMIT_HDR, "1");
        }
    }
}
