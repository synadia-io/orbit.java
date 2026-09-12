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

    /** The batch committed and the server answered with the authoritative PublishAck. */
    Committed,

    /** The server reported a gap while in {@link GapMode#Fail}, which abandons the batch. */
    Gap,

    /** The server reported a per message error while in {@link GapMode#Fail}. */
    Error,

    /** The client gave up, through {@code abandon()} or {@code close()}. */
    Abandoned
}
