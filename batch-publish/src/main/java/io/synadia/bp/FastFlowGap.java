// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.support.JsonValue;

import static io.nats.client.support.ApiConstants.LAST_SEQ;
import static io.nats.client.support.ApiConstants.SEQ;
import static io.nats.client.support.JsonValueUtils.readLong;

/**
 * Reports that the server detected a gap in a fast ingest batch, meaning one or more messages
 * were dropped or lost across a stream leader change.
 * <p>
 * ADR-50 documents this as informational and losable. Only the final
 * {@link io.nats.client.api.PublishAck} is authoritative about what was persisted. A gap is sent
 * the instant the server detects it, so it arrives out of order with respect to flow acks and
 * must never be used to move the acknowledged sequence or the flow rate.
 */
public class FastFlowGap {
    private final long lastSequence;
    private final long sequence;

    FastFlowGap(JsonValue jv) {
        lastSequence = readLong(jv, LAST_SEQ, 0);
        sequence = readLong(jv, SEQ, 0);
    }

    /**
     * The last batch sequence the server received before the gap.
     * @return the last received batch sequence
     */
    public long getLastSequence() {
        return lastSequence;
    }

    /**
     * The batch sequence the server has resumed from.
     * @return the current batch sequence
     */
    public long getSequence() {
        return sequence;
    }

    @Override
    public String toString() {
        return "FastFlowGap{lastSequence=" + lastSequence + ", sequence=" + sequence + '}';
    }
}
