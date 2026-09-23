// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

/**
 * Why a fast ingest batch ended, or that it has not.
 * <p>
 * A batch that ends on a gap or an error is over on the server as well as the client, and
 * whatever it had already persisted stays persisted. A batch that is abandoned is over only on
 * the client; the server drops it on its own inactivity timeout.
 */
public enum EndReason {
    /** The batch is still running. */
    Open,

    /**
     * The batch was closed with {@code closeBatch}, which ADR-50 calls a commit, and the server
     * answered with the authoritative PublishAck.
     */
    Committed,

    /** The server reported a gap while in {@link GapMode#Fail}, which abandons the batch. */
    Gap,

    /**
     * The server reported an error that ended the batch: a per message error while in
     * {@link GapMode#Fail}, or an error in place of the terminal PublishAck in either mode, such as
     * {@code 10208 batch publish ID unknown} when the server had already dropped the batch.
     */
    Error,

    /**
     * The client gave up: {@code abandon()}, {@code close()}, or a failure that leaves the batch
     * unusable - a first message the server never answered, or a commit whose acknowledgement
     * never arrived.
     */
    Abandoned
}
