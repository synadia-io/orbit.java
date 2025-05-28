// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

import io.nats.client.api.KeyResult;

public class EncodedKeyResult<KeyType, DataType> {

    private final KeyResult keyResult;
    private final Codec<KeyType, DataType> codec;

    public EncodedKeyResult(KeyResult keyResult, Codec<KeyType, DataType> codec) {
        this.keyResult = keyResult;
        this.codec = codec;
    }

    public KeyType getKey() throws Exception {
        String key = keyResult.getKey();
        return key == null ? null : codec.decodeKey(key);
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
