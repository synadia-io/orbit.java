// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The exception thrown when a batch publish fails. It always carries the id of the batch
 * that failed, and when the failure was reported by the server it also carries the
 * underlying {@link JetStreamApiException} so the error code and description are available.
 */
public class BatchPublishException extends Exception {
    /** The server's api exception, or null when the failure did not come from the server. */
    private final JetStreamApiException jsApiException;

    /** The id of the batch that failed. */
    private final String batchId;

    /**
     * Construct an exception with a message.
     * @param batchId the id of the batch that failed
     * @param message the message describing the failure
     */
    public BatchPublishException(@NonNull String batchId, @NonNull String message) {
        super(message);
        this.batchId = batchId;
        jsApiException = null;
    }

    /**
     * Construct an exception for a failure the server reported, keeping the api exception
     * so the error code and description can be read from it.
     * @param batchId the id of the batch that failed
     * @param cause the api exception from the server
     */
    public BatchPublishException(@NonNull String batchId, @NonNull JetStreamApiException cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = cause;
    }

    /**
     * Construct an exception from any other cause.
     * @param batchId the id of the batch that failed
     * @param cause the underlying exception
     */
    public BatchPublishException(@NonNull String batchId, @NonNull Throwable cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = null;
    }

    @Override
    public String getMessage() {
        return "[" + batchId + "] " + super.getMessage();
    }

    /**
     * Get the id of the batch that failed.
     * @return the batch id
     */
    @NonNull
    public String getBatchId() {
        return batchId;
    }

    /**
     * Get the api exception the server reported, if the failure came from the server.
     * @return the api exception or null
     */
    @Nullable
    public JetStreamApiException getJsApiException() {
        return jsApiException;
    }

    /**
     * Get the error code from the response if the exception is a JetStreamApiException
     * otherwise will be -1
     * @return the code
     */
    public int getErrorCode() {
        return jsApiException == null ? -1 : jsApiException.getErrorCode();
    }

    /**
     * Get the error code from the response if the exception is a JetStreamApiException
     * otherwise will be -1
     * @return the code
     */
    public int getApiErrorCode() {
        return jsApiException == null ? -1 : jsApiException.getApiErrorCode();
    }

    /**
     * Get the description from the response if the exception is a JetStreamApiException
     * otherwise will be null
     * @return the description
     */
    @Nullable
    public String getErrorDescription() {
        return jsApiException == null ? null : jsApiException.getErrorDescription();
    }
}
