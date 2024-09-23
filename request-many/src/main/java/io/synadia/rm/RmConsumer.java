// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.rm;

import io.nats.client.Message;

public interface RmConsumer {
    boolean consume(Message message);
}
