// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyResult;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EncodedKeyConsumer<KeyType, DataType> {
    private final LinkedBlockingQueue<KeyResult> queue;
    private final Codec<KeyType, DataType> codec;

    public EncodedKeyConsumer(LinkedBlockingQueue<KeyResult> queue, Codec<KeyType, DataType> codec) {
        this.queue = queue;
        this.codec = codec;
    }

    private EncodedKeyResult<KeyType, DataType> convert(KeyResult result) {
        return result == null ? null : new EncodedKeyResult<>(result, codec);
    }

    public EncodedKeyResult<KeyType, DataType> take() throws InterruptedException {
        return convert(queue.take());
    }

    public EncodedKeyResult<KeyType, DataType> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return convert(queue.poll(timeout, unit));
    }

    public EncodedKeyResult<KeyType, DataType> poll() throws InterruptedException {
        return convert(queue.poll());
    }

    public EncodedKeyResult<KeyType, DataType> peek() {
        return convert(queue.peek());
    }
}
