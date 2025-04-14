// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

public enum RetryCondition {

    /**
     * Any 503 No Responder
     */
    NoResponders,

    /**
     * Any IOException like a timeout / disconnect
     */
    IoEx,

    /**
     * Any JetStreamApiException
     */
    JetStreamApiEx,

    /**
     * Any RuntimeException
     */
    RuntimeEx
}
