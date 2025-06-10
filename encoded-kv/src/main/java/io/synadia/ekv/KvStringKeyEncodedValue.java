// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.synadia.ekv.codec.StringKeyCodec;
import io.synadia.ekv.codec.ValueCodec;

import java.io.IOException;

public class KvStringKeyEncodedValue<ValueType> extends KvEncodedKeyEncodedValue<String, ValueType> {
    public KvStringKeyEncodedValue(Connection connection, String bucketName, ValueCodec<ValueType> valueCodec) throws IOException {
        super(connection, bucketName, new StringKeyCodec(), valueCodec);
    }

    public KvStringKeyEncodedValue(Connection connection, String bucketName, ValueCodec<ValueType> valueCodec, KeyValueOptions kvo) throws IOException {
        super(connection, bucketName, new StringKeyCodec(), valueCodec, kvo);
    }

    public KvStringKeyEncodedValue(KeyValue kv, ValueCodec<ValueType> valueCodec) {
        super(kv, new StringKeyCodec(), valueCodec);
    }
}
