// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.api.ApiResponse;
import io.nats.client.support.JsonValue;

import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonValueUtils.readLong;

/**
 * Reports that one message in a fast ingest batch failed a per message header check, such as
 * {@code Nats-Expected-Last-Sequence}.
 * <p>
 * ADR-50 deliberately keeps this out of the PublishAck: a PublishAck carries either an error or
 * the persisted state, never both. Reporting the failure separately lets the client learn both
 * that a given sequence failed and which sequences were persisted, and lets it surface the error
 * the instant the server sees it rather than at the end of the batch.
 * <p>
 * Extends {@link ApiResponse} so the nested error object parses for free, which is where
 * {@link #getApiErrorCode()} and {@link #getDescription()} come from.
 */
public class FastFlowError extends ApiResponse<FastFlowError> {
    private final long sequence;

    FastFlowError(JsonValue jv) {
        super(jv);
        sequence = readLong(jv, SEQ, 0);
    }

    /**
     * The batch sequence of the message that failed.
     * @return the batch sequence
     */
    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "FastFlowError{sequence=" + sequence + ", code=" + getApiErrorCode() + ", description=" + getDescription() + '}';
    }
}
