// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.rm;

/**
 * This class is EXPERIMENTAL, meaning it's api is subject to change.
 * This interface is used by the RequestMany request method to
 * <ul>
 * <li>Give messages to the user as the come in over the wire</li>
 * <li>For the user to indicate to continue or stop waiting for messages. If the user is not using the sentinel pattern, they should always return true.</li>
 * </ul>
 */
public interface RmHandler {
    /**
     * Accept a message from the request method.
     * @param message the message. It may be an end of data (EOD) marker
     * @return true if the RequestMany should continue to wait for messages. False to stop waiting.
     */
    boolean handle(RmMessage message);
}
