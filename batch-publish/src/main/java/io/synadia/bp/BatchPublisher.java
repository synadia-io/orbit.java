// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.support.Validator.validateNotNull;

public class BatchPublisher {
    private final Connection conn;
    private final Duration requestTimeout;
    private int lastSeq;
    private Headers openedHeaders;
    private List<String> lastUserHeaderKeys;

    /**
     * Construct a BatchPublisher instance.
     * @param conn the connection to operate under
     */
    public BatchPublisher(Connection conn) {
        this(conn, conn.getOptions().getConnectionTimeout());
    }

    /**
     * Construct a BatchPublisher instance.
     * @param conn the connection to operate under
     * @param jso a JetStreamOptions instance to extract the request timeout
     */
    public BatchPublisher(Connection conn, JetStreamOptions jso) {
        this(conn, jso.getRequestTimeout());
    }

    public BatchPublisher(Connection conn, Duration requestTimeout) {
        validateNotNull(conn, "Connection required,");
        if (!conn.getServerInfo().isNewerVersionThan("2.11.99")) {
            throw new IllegalArgumentException("Batch direct get not available until server version 2.11.0.");
        }
        this.conn = conn;
        this.requestTimeout = requestTimeout;
        lastSeq = 0;
    }

    public String getBatchId() {
        if (openedHeaders == null) {
            return null;
        }
        return openedHeaders.getFirst(NatsJetStreamConstants.NATS_BATCH_ID_HDR);
    }

    public boolean open(String subject, byte[] data) throws BatchPublishException {
        return open(new NUID().next(), subject, null, data);
    }

    public boolean open(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        return open(new NUID().next(), subject, null, data);
    }

    public boolean open(String batchId, String subject, byte[] data) throws BatchPublishException {
        validateNotNull(batchId, "Batch ID");
        return open(new NUID().next(), subject, null, data);
    }

    public boolean open(String batchId, String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        validateNotNull(batchId, "Batch ID");
        lastSeq = 0;
        openedHeaders = new Headers();
        openedHeaders.put(NatsJetStreamConstants.NATS_BATCH_ID_HDR, batchId);
        lastUserHeaderKeys = new ArrayList<>();
        return publishConfirm(subject, userHeaders, data);
    }

    public void publish(String subject, byte[] data) {
        publish(subject, null, data);
    }

    public void publish(String subject, Headers userHeaders, byte[] data) {
        if (openedHeaders == null) {
            throw new IllegalStateException("Batch not opened");
        }
        updateHeaders(userHeaders, false);
        conn.publish(subject, openedHeaders, data);
    }

    public boolean publishConfirm(String subject, byte[] data) throws BatchPublishException {
        request(subject, null, data, false);
        return true;
    }

    public boolean publishConfirm(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        request(subject, userHeaders, data, false);
        return true;
    }

    public PublishAck publishLast(String subject, byte[] data) throws BatchPublishException {
        return publishLast(subject, null, data);
    }

    public PublishAck publishLast(String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        try {
            return new PublishAck(request(subject, userHeaders, data, true));
        }
        catch (JetStreamApiException | IOException e) {
            throw new BatchPublishException(e);
        }
        finally {
            openedHeaders = null; // closes the batch
        }
    }

    private Message request(String subject, Headers userHeaders, byte[] data, boolean commit) throws BatchPublishException {
        if (openedHeaders == null) {
            throw new IllegalStateException("Batch not opened");
        }
        try {
            updateHeaders(userHeaders, commit);
            CompletableFuture<Message> f = conn.requestWithTimeout(subject, openedHeaders, data, requestTimeout);
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
            openedHeaders.remove(lastUserHeaderKeys);
            lastUserHeaderKeys.clear();
        }
        if (userHeaders != null && !userHeaders.isEmpty()) {
            Set<String> keys = userHeaders.keySet();
            for (String key : keys) {
                openedHeaders.add(key, userHeaders.get(key));
                lastUserHeaderKeys.add(key);
            }
        }
        openedHeaders.put(NatsJetStreamConstants.NATS_BATCH_SEQUENCE_HDR, Integer.toString(++lastSeq));
        if (commit) {
            openedHeaders.put(NatsJetStreamConstants.NATS_BATCH_COMMIT_HDR, "1");
        }
    }
}
