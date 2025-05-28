// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.nats.client.api.KeyValueEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EncodedKeyValue<KeyType, DataType> {
    private final KeyValue kv;
    private final Codec<KeyType, DataType> codec;

    public EncodedKeyValue(Connection connection, String bucketName, Codec<KeyType, DataType> codec) throws IOException {
        this(connection, bucketName, codec, null);
    }

    public EncodedKeyValue(Connection connection, String bucketName, Codec<KeyType, DataType> codec, KeyValueOptions kvo) throws IOException {
        this.codec = codec;
        if (kvo == null) {
            kv = connection.keyValue(bucketName);
        }
        else {
            kv = connection.keyValue(bucketName, kvo);
        }
    }

    public EncodedKeyValueEntry<KeyType, DataType> get(KeyType key) throws IOException, JetStreamApiException {
        return _get(kv.get(codec.encodeKey(key)));
    }

    public EncodedKeyValueEntry<KeyType, DataType> get(KeyType key, long revision) throws IOException, JetStreamApiException {
        return _get(kv.get(codec.encodeKey(key), revision));
    }

    private EncodedKeyValueEntry<KeyType, DataType> _get(KeyValueEntry kve) {
        if (kve == null) {
            return null;
        }
        return new EncodedKeyValueEntry<>(kve, codec);
    }

    public List<EncodedKeyValueEntry<KeyType, DataType>> history(KeyType key) throws IOException, JetStreamApiException, InterruptedException {
        List<KeyValueEntry> entries = kv.history(codec.encodeKey(key));
        List<EncodedKeyValueEntry<KeyType, DataType>> encodedEntries = new ArrayList<>();
        for (KeyValueEntry kve: entries) {
            encodedEntries.add(new EncodedKeyValueEntry<>(kve, codec));
        }
        return encodedEntries;
    }

    public long put(KeyType key, DataType value) throws IOException, JetStreamApiException {
        return kv.put(codec.encodeKey(key), codec.encodeData(value));
    }

    public long create(KeyType key, DataType value) throws IOException, JetStreamApiException {
        return kv.create(codec.encodeKey(key), codec.encodeData(value));
    }

    public long update(KeyType key, DataType value, long expectedRevision) throws IOException, JetStreamApiException {
        return kv.update(codec.encodeKey(key), codec.encodeData(value), expectedRevision);
    }

    public void delete(KeyType key) throws IOException, JetStreamApiException {
        kv.delete(codec.encodeKey(key));
    }

    public void delete(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        kv.delete(codec.encodeKey(key), expectedRevision);
    }

    public void purge(KeyType key) throws IOException, JetStreamApiException {
        kv.purge(codec.encodeKey(key));
    }

    public void purge(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        kv.purge(codec.encodeKey(key), expectedRevision);
    }
}
