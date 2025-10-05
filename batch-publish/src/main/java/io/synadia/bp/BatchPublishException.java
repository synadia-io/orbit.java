// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public class BatchPublishException extends Exception {
    private final JetStreamApiException jsApiException;
    private final String batchId;

    public BatchPublishException(@NonNull String batchId, @NonNull String message) {
        super(message);
        this.batchId = batchId;
        jsApiException = null;
    }

    public BatchPublishException(@NonNull String batchId, @NonNull JetStreamApiException cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = cause;
    }

    public BatchPublishException(@NonNull String batchId, @NonNull Throwable cause) {
        super(cause);
        this.batchId = batchId;
        jsApiException = null;
    }

    @Override
    public String getMessage() {
        return "[" + batchId + "] " + super.getMessage();
    }

    @NonNull
    public String getBatchId() {
        return batchId;
    }

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
