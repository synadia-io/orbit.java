// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

public interface Codec<KeyType, DataType> {
    String encodeKey(KeyType key);

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    default boolean allowsFiltering() { return false; }

    default String encodeFilter(KeyType filter) {
        throw new UnsupportedOperationException("Filter encoding not supported");
    }

    byte[] encodeData(DataType value);

    KeyType decodeKey(String key);

    DataType decodeData(byte[] data);
}
