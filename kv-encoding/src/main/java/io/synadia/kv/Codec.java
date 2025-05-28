// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

public interface Codec<KeyType, DataType> {
    String encodeKey(KeyType key);
    byte[] encodeData(DataType value);
    KeyType decodeKey(String key) throws Exception;
    DataType decodeData(byte[] data) throws Exception;
}
