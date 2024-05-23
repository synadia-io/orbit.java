// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

/**
 *
 */
public interface PublisherListener {
    void published(Flight flight);
    void acked(Flight flight);
    void completedExceptionally(Flight flight);
    void timeout(Flight flight);
}
