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
}
