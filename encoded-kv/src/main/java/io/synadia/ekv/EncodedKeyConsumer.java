// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyResult;
import io.synadia.ekv.codec.KeyCodec;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EncodedKeyConsumer<KeyType> {
    private final LinkedBlockingQueue<KeyResult> queue;
    final KeyCodec<KeyType> keyCodec;

    public EncodedKeyConsumer(LinkedBlockingQueue<KeyResult> queue, KeyCodec<KeyType> keyCodec) {
        this.queue = queue;
        this.keyCodec = keyCodec;
    }

    private EncodedKeyResult<KeyType> convert(KeyResult result) {
        return result == null ? null : new EncodedKeyResult<>(result, keyCodec);
    }

    public EncodedKeyResult<KeyType> take() throws InterruptedException {
        return convert(queue.take());
    }

    public EncodedKeyResult<KeyType> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return convert(queue.poll(timeout, unit));
    }

    public EncodedKeyResult<KeyType> poll() throws InterruptedException {
        return convert(queue.poll());
    }

    public EncodedKeyResult<KeyType> peek() {
        return convert(queue.peek());
    }
}
