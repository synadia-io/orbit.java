// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.synadia.ekv.codec.ByteValueCodec;
import io.synadia.ekv.codec.KeyCodec;

import java.io.IOException;

public class KvEncodedKey<KeyType> extends KvEncoded<KeyType, byte[]> {
    public KvEncodedKey(Connection connection, String bucketName, KeyCodec<KeyType> keyCodec) throws IOException {
        super(connection, bucketName, keyCodec, new ByteValueCodec());
    }

    public KvEncodedKey(Connection connection, String bucketName, KeyCodec<KeyType> keyCodec, KeyValueOptions kvo) throws IOException {
        super(connection, bucketName, keyCodec, new ByteValueCodec(), kvo);
    }

    public KvEncodedKey(KeyValue kv, KeyCodec<KeyType> keyCodec) {
        super(kv, keyCodec, new ByteValueCodec());
    }
}
