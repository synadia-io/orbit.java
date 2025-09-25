// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import org.jspecify.annotations.Nullable;

public class BatchPublishException extends Exception {
    private final JetStreamApiException jsApiException;

    public BatchPublishException(String message) {
        super(message);
        jsApiException = null;
    }

    public BatchPublishException(JetStreamApiException cause) {
        super(cause);
        jsApiException = cause;
    }

    public BatchPublishException(Throwable cause) {
        super(cause);
        jsApiException = null;
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
