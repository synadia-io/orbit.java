// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import io.nats.client.api.PublishAck;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Thrown when a fast ingest batch cannot proceed. Mirrors {@link BatchPublishException}.
 */
public class FastPublishException extends Exception {
    /** The terminal PublishAck of a batch the server ended, when one arrived. */
    private PublishAck publishAck;

    /** The underlying JetStream api exception, when the failure came from one. */
    private final JetStreamApiException jsApiException;

    /** The id of the batch that failed. */
    private final String batchId;

    /**
     * Construct with a message.
     * @param batchId the batch id
     * @param message the message
     */
    public FastPublishException(@NonNull String batchId, @NonNull String message) {
        super(message);
        this.batchId = batchId;
        jsApiException = null;
    }

    /**
     * Construct from a JetStreamApiException, preserving its error codes.
     * @param batchId the batch id
     * @param cause the cause
     */
    public FastPublishException(@NonNull String batchId, @NonNull JetStreamApiException cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = cause;
    }

    /**
     * Construct from any other cause.
     * @param batchId the batch id
     * @param cause the cause
     */
    public FastPublishException(@NonNull String batchId, @NonNull Throwable cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = null;
    }

    @Override
    public String getMessage() {
        return "[" + batchId + "] " + super.getMessage();
    }

    /**
     * The id of the batch that failed.
     * @return the batch id
     */
    @NonNull
    public String getBatchId() {
        return batchId;
    }

    /**
     * The underlying JetStreamApiException if there was one.
     * @return the exception or null
     */
    @Nullable
    public JetStreamApiException getJsApiException() {
        return jsApiException;
    }

    /**
     * The terminal PublishAck of a batch the server ended under the client, if it arrived.
     * <p>
     * When a gap or a per message error ends a {@link GapMode#Fail} batch, the server abandons
     * the batch and sends a final PublishAck reporting how far it actually got. ADR-50 makes
     * that ack the only authoritative statement of what was persisted, a gap report explicitly
     * not being one, so it is collected and attached here rather than discarded. Null when the
     * batch ended some other way, and null when the ack never arrived, which ADR-50 allows
     * because these acks are best effort.
     * @return the terminal PublishAck or null
     */
    @Nullable
    public PublishAck getPublishAck() {
        return publishAck;
    }

    void setPublishAck(PublishAck publishAck) {
        this.publishAck = publishAck;
    }

    /**
     * The error code from the response if this came from a JetStreamApiException, otherwise -1.
     * @return the code
     */
    public int getErrorCode() {
        return jsApiException == null ? -1 : jsApiException.getErrorCode();
    }

    /**
     * The api error code from the response if this came from a JetStreamApiException, otherwise -1.
     * @return the code
     */
    public int getApiErrorCode() {
        return jsApiException == null ? -1 : jsApiException.getApiErrorCode();
    }

    /**
     * The description from the response if this came from a JetStreamApiException, otherwise null.
     * @return the description
     */
    @Nullable
    public String getErrorDescription() {
        return jsApiException == null ? null : jsApiException.getErrorDescription();
    }
}
