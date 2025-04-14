// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
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
    void published(InFlight flight);

    /**
     * The message has been acked
     * @param flight the flight representing the message
     */
    void acked(PostFlight flight);

    /**
     * The message has completed exceptionally, for instance a 503 No Responders.
     * Timeouts are notified via the timeout method.
     * @param flight the flight representing the message
     */
    void completedExceptionally(PostFlight flight);

    /**
     * The message has internally timed out waiting for the ack. Usually a sign of
     * lost connection.
     * @param flight the flight representing the message
     */
    void timeout(PostFlight flight);

    /**
     * The engine has just paused publishing waiting for inflight to
     * drop below the resumeInFlightAmount
     * @param currentInFlight the number of messages in flight
     * @param maxInFlight the number of in flight messages when publishing will be paused
     * @param resumeAmount the number of in flight messages when publishing will resume after being paused
     */
    void paused(int currentInFlight, int maxInFlight, int resumeAmount);

    /**
     * The engine has just resumed publishing and will continue unless
     * the number of messages in flight reaches the max
     * @param currentInFlight the number of messages in flight
     * @param maxInFlight the number of in flight messages when publishing will be paused
     * @param resumeAmount the number of in flight messages when publishing will resume after being paused
     */
    void resumed(int currentInFlight, int maxInFlight, int resumeAmount);
}
