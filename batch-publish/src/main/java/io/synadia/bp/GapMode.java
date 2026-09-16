// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import static io.nats.client.support.NatsJetStreamConstants.FAST_BATCH_GAP_FAIL;
import static io.nats.client.support.NatsJetStreamConstants.FAST_BATCH_GAP_OK;

/**
 * How a fast ingest batch reacts when the server detects a gap, meaning one or more messages
 * were dropped by the server's overload protection or lost across a stream leader change.
 * The mode is stated once in the reply subject when the batch starts and cannot be changed later.
 */
public enum GapMode {
    /**
     * Gaps are reported to the listener and the batch continues from the received sequence.
     * Per message header check failures are also only reported, not fatal.
     * This is what a metrics firehose wants.
     */
    Ok(FAST_BATCH_GAP_OK),

    /**
     * Any gap abandons the batch. The server stops accepting messages and sends a final
     * PublishAck reporting how far it got. Per message header check failures also stop the batch.
     * This is what a use case like ObjectStore needs, where a gap is a hole in a file.
     */
    Fail(FAST_BATCH_GAP_FAIL);

    private final String wire;

    GapMode(String wire) {
        this.wire = wire;
    }

    /**
     * The token used for this mode in the reply subject.
     * @return the wire token, "ok" or "fail"
     */
    @Override
    public String toString() {
        return wire;
    }
}
