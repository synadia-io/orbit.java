// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv.codec;

public class TestStringKeyCodec extends StringKeyCodec<KeyOrValue> {
    @Override
    public byte[] encodeData(KeyOrValue value) {
        return value == null ? null : value.serialize();
    }

    @Override
    public KeyOrValue decodeData(byte[] data) throws Exception {
        return data == null || data.length == 0 ? null : new KeyOrValue(data);
    }
}
