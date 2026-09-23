// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

/**
 * The result of adding one message to a fast ingest batch.
 * This is not a JetStream PublishAck. A fast batch produces exactly one authoritative
 * {@link io.nats.client.api.PublishAck}, at {@code closeBatch}. This type only reports where the
 * batch stands locally.
 */
public class FastPubAck {
    private final long batchSequence;
    private final long ackSequence;

    FastPubAck(long batchSequence, long ackSequence) {
        this.batchSequence = batchSequence;
        this.ackSequence = ackSequence;
    }

    /**
     * The batch sequence assigned to the message that was just added.
     * @return the batch sequence
     */
    public long getBatchSequence() {
        return batchSequence;
    }

    /**
     * The highest batch sequence the server has acknowledged so far. Acks are cumulative and
     * arrive every N messages, so this normally trails {@link #getBatchSequence()}.
     * @return the acknowledged batch sequence
     */
    public long getAckSequence() {
        return ackSequence;
    }

    @Override
    public String toString() {
        return "FastPubAck{batchSequence=" + batchSequence + ", ackSequence=" + ackSequence + '}';
    }
}
