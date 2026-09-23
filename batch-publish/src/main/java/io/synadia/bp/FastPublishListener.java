// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

/**
 * Callbacks for the informational messages a fast ingest batch receives on its control channel.
 * All methods default to doing nothing, so an implementation only overrides what it cares about.
 * <p>
 * These are called on the thread that called {@code add}, {@code closeBatch}, {@code close} or
 * {@code ping}, never on a separate thread. A publisher that goes quiet will not deliver anything
 * until it publishes again, so a batch that must notice gaps promptly should call
 * {@link FastPublisher#ping()} periodically.
 */
public interface FastPublishListener {
    /**
     * The server detected a gap. In {@link GapMode#Fail} the batch is over and the final
     * PublishAck is still coming; in {@link GapMode#Ok} the batch continues.
     * @param gap the gap report
     */
    default void onGap(FastFlowGap gap) {}

    /**
     * A message failed a per message header check. In {@link GapMode#Fail} the batch is over;
     * in {@link GapMode#Ok} the batch continues.
     * @param error the error report
     */
    default void onError(FastFlowError error) {}

    /**
     * The server changed the flow rate, meaning how often it will acknowledge. The server may
     * raise it, usually doubling toward the maximum the client asked for, or lower it, usually
     * halving with a floor of 1, based on how loaded the stream is.
     * @param ackEvery the new number of messages between acknowledgements
     */
    default void onFlowChange(long ackEvery) {}
}
