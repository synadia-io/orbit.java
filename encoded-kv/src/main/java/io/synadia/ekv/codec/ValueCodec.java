// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

public interface ValueCodec<ValueType> {
    byte[] encode(ValueType value);
    ValueType decode(byte[] encodedValue);
}
