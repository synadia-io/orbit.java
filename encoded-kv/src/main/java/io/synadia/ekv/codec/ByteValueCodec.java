// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

public class ByteValueCodec implements ValueCodec<byte[]> {

    @Override
    public byte[] encode(byte[] value) {
        return value;
    }

    @Override
    public byte[] decode(byte[] encodedValue) {
        return encodedValue;
    }
}
