// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

/**
 * The interface is designed to listen to events as the AsyncJsPublish runs
 */
public interface AsyncJsPublishListener {
    /**
     * The message has been published
     * @param flight the flight representing the message
     */
    void published(Flight flight);

    /**
     * The message has been acked
     * @param flight the flight representing the message
     */
    void acked(Flight flight);

    /**
     * The message has completed exceptionally, for instance a 503 No Responders.
     * Timeouts are notified via the timeout method.
     * @param flight the flight representing the message
     */
    void completedExceptionally(Flight flight);

    /**
     * The message has internally timed out waiting for the ack. Usually a sign of
     * lost connection.
     * @param flight the flight representing the message
     */
    void timeout(Flight flight);
}
