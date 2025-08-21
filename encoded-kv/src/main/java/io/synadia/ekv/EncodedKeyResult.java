// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyResult;
import io.synadia.ekv.codec.KeyCodec;

public class EncodedKeyResult<KeyType> {

    private final KeyResult keyResult;
    private final KeyCodec<KeyType> keyCodec;

    public EncodedKeyResult(KeyResult keyResult, KeyCodec<KeyType> keyCodec) {
        this.keyResult = keyResult;
        this.keyCodec = keyCodec;
    }

    public KeyType getKey() {
        String key = keyResult.getKey();
        return key == null ? null : keyCodec.decode(key);
    }

    public Exception getException() {
        return keyResult.getException();
    }

    public boolean isKey() {
        return keyResult.isKey();
    }

    public boolean isException() {
        return keyResult.isException();
    }

    public boolean isDone() {
        return keyResult.isDone();
    }
}
