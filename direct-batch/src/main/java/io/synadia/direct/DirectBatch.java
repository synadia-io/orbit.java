// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.direct;

import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.Status;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.JetStreamOptions.DEFAULT_JS_OPTIONS;
import static io.nats.client.support.NatsJetStreamConstants.JSAPI_DIRECT_GET;
import static io.nats.client.support.Validator.required;
import static io.nats.client.support.Validator.validateNotNull;

public class DirectBatch {
    private final Connection conn;
    private final JetStreamOptions jso;
    private final String streamName;
    final Duration timeout;

    public DirectBatch(Connection conn, String streamName) throws IOException, JetStreamApiException {
        this(conn, null, streamName);
    }

    public DirectBatch(Connection conn, JetStreamOptions jso, String streamName) throws IOException, JetStreamApiException {
        validateNotNull(conn, "Connection required,");
        if (!conn.getServerInfo().isNewerVersionThan("2.10.99")) {
            throw new IllegalArgumentException("Batch direct get not available until server version 2.11.0.");
        }
        this.conn = conn;
        this.jso = jso == null ? DEFAULT_JS_OPTIONS : jso;
        JetStreamManagement jsm = conn.jetStreamManagement(this.jso);

        this.streamName = required(streamName, "Stream name required,");
        StreamInfo si = jsm.getStreamInfo(streamName);
        if (!si.getConfiguration().getAllowDirect()) {
            throw new IllegalArgumentException("Stream must have allow direct set.");
        }

        timeout = this.jso.getRequestTimeout() == null ? conn.getOptions().getConnectionTimeout() : this.jso.getRequestTimeout();
    }

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * @param messageBatchGetRequest the request details
     * @return a list containing {@link MessageInfo}
     */
    public List<MessageInfo> fetchMessageBatch(MessageBatchGetRequest messageBatchGetRequest) {
        validateNotNull(messageBatchGetRequest, "Message Batch Get Request");
        final List<MessageInfo> results = new ArrayList<>();
        _requestMessageBatch(messageBatchGetRequest, false, mi -> {
            if (mi.isErrorStatus()) {
                results.clear();
            }
            results.add(mi);
        });
        return results;
    }

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * @param messageBatchGetRequest the request details
     * @return a queue used to asynchronously receive {@link MessageInfo}
     */
    public LinkedBlockingQueue<MessageInfo> queueMessageBatch(MessageBatchGetRequest messageBatchGetRequest) {
        validateNotNull(messageBatchGetRequest, "Message Batch Get Request");
        final LinkedBlockingQueue<MessageInfo> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(
            () -> _requestMessageBatch(messageBatchGetRequest, true, q::add));
        return q;
    }

    /**
     * Request a batch of messages using a {@link MessageBatchGetRequest}.
     * <p>
     * @param messageBatchGetRequest the request details
     * @param handler                the handler used for receiving {@link MessageInfo}
     * @return true if all messages were received and properly terminated with a server EOB
     */
    public boolean requestMessageBatch(MessageBatchGetRequest messageBatchGetRequest, MessageInfoHandler handler) {
        validateNotNull(messageBatchGetRequest, "Message Batch Get Request");
        return _requestMessageBatch(messageBatchGetRequest, true, handler);
    }

    private String prependPrefix(String subject) {
        return jso.getPrefix() + subject;
    }

    private boolean _requestMessageBatch(MessageBatchGetRequest mbgr, boolean sendEob, MessageInfoHandler handler) {
        Subscription sub = null;

        try {
            String replyTo = conn.createInbox();
            sub = conn.subscribe(replyTo);

            String subject = prependPrefix(String.format(JSAPI_DIRECT_GET, streamName));
            conn.publish(subject, replyTo, mbgr.serialize());

            while (true) {
                Message msg = sub.nextMessage(timeout);
                Status errorOrNonEob = null;
                if (msg == null) {
                    errorOrNonEob = Status.TIMEOUT_OR_NO_MESSAGES;
                }
                else if (msg.isStatusMessage()) {
                    if (msg.getStatus().isEob()) {
                        return true;  // will send eob in finally if caller asked
                    }
                    errorOrNonEob = msg.getStatus();
                }

                if (errorOrNonEob != null) {
                    // All error or non eob statuses, always send, but it is the last message to the caller
                    sendEob = false;
                    handler.onMessageInfo(new MessageInfo(errorOrNonEob, streamName));
                    return false; // should not time out before eob
                }

                MessageInfo messageInfo = new MessageInfo(msg, streamName, true);
                handler.onMessageInfo(messageInfo);
            }
        }
        catch (InterruptedException e) {
            // sub.nextMessage was fetching one message
            // and data is not completely read
            // so it seems like this is an error condition
            Thread.currentThread().interrupt();
            sendEob = false;
            return false;
        } finally {
            if (sendEob) {
                try {
                    handler.onMessageInfo(new MessageInfo(Status.EOB, streamName));
                }
                catch (RuntimeException ignore) { /* user handler runtime error */ }
            }
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            } catch (RuntimeException ignore) { /* don't want this to fail here */ }
        }
    }
}
