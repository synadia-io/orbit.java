// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.Connection;

/**
 * Static information about batch publishing that is not carried on any publisher.
 */
public class BatchUtils {
    private BatchUtils() {} // this class is not instantiated

    /**
     * The maximum number of messages an atomic batch may contain on this connection's server.
     * <p>
     * Hardcoded to 1000 today, the number ADR-50 documents, because the limit is documented
     * rather than advertised: the server takes it from its {@code max_batch_size} option and
     * reports it in neither INFO nor stream info, so a client has no way to ask. The connection
     * is a parameter so that this can change without changing callers, if a later server
     * publishes the value. The answer cannot change for a connection once established, so an
     * implementation that has to ask the server may cache it.
     * <p>
     * The count includes the message that carries the commit, when the batch ends by storing one.
     * A batch ending with an EOB sentinel does not count the sentinel, so it may hold this many
     * messages rather than one fewer.
     * @param conn the connection whose server the limit applies to
     * @return the maximum number of messages
     */
    public static int getMaxBatchSize(Connection conn) {
        return 1000;
    }

    /**
     * The percentage of the server's batch idle timeout used as the default idle ping interval.
     * Half the timeout leaves room for one lost ping before the server abandons the batch.
     */
    public static final int DEFAULT_IDLE_PING_PERCENT = 50;

    /**
     * The largest percentage of the server's batch idle timeout that an idle ping interval may be.
     */
    public static final int MAX_IDLE_PING_PERCENT = 80;

    /**
     * How many seconds a batch may go without receiving a message before the server abandons it,
     * on this connection's server. A fast ingest ping counts as a message.
     * <p>
     * Hardcoded to 10 today, the number ADR-50 documents, for the same reason as
     * {@link #getMaxBatchSize(Connection)}: the server takes it from its
     * {@code jetstream { limits { batch { timeout } } } } option and reports it in neither INFO nor
     * stream info, only in the monitoring endpoints, so a client has no way to ask. The connection
     * is a parameter so that this can change without changing callers, if a later server
     * publishes the value.
     * <p>
     * The server sends nothing when it abandons a batch this way. The next message the client
     * sends for it is answered {@code 10208 batch publish ID unknown}.
     * @param conn the connection whose server the timeout applies to
     * @return the timeout in seconds
     */
    public static int getBatchIdleTimeoutSeconds(Connection conn) {
        return 10;
    }

    /**
     * The idle ping interval a fast publisher uses when none is set:
     * {@value #DEFAULT_IDLE_PING_PERCENT}% of {@link #getBatchIdleTimeoutSeconds(Connection)},
     * rounded down. 5 seconds today.
     * @param conn the connection whose server the timeout applies to
     * @return the default idle ping interval in seconds
     */
    public static int getDefaultIdlePingSeconds(Connection conn) {
        return getBatchIdleTimeoutSeconds(conn) * DEFAULT_IDLE_PING_PERCENT / 100;
    }

    /**
     * The largest idle ping interval a fast publisher accepts:
     * {@value #MAX_IDLE_PING_PERCENT}% of {@link #getBatchIdleTimeoutSeconds(Connection)},
     * rounded down. 8 seconds today.
     * @param conn the connection whose server the timeout applies to
     * @return the maximum idle ping interval in seconds
     */
    public static int getMaxIdlePingSeconds(Connection conn) {
        return getBatchIdleTimeoutSeconds(conn) * MAX_IDLE_PING_PERCENT / 100;
    }
}
