// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueOptions;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.KeyResult;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.impl.NatsKeyValueAdapter;
import io.nats.client.support.NatsKeyValueUtil;
import io.synadia.kv.codec.Codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.support.NatsKeyValueUtil.getOperation;

public class EncodedKeyValue<KeyType, DataType> {
    private final Connection connection;
    private final NatsKeyValueAdapter adapter;
    private final Codec<KeyType, DataType> codec;

    public EncodedKeyValue(Connection connection, String bucketName, Codec<KeyType, DataType> codec) throws IOException {
        this(connection, bucketName, codec, null);
    }

    public EncodedKeyValue(Connection connection, String bucketName, Codec<KeyType, DataType> codec, KeyValueOptions kvo) throws IOException {
        this.connection = connection;
        this.codec = codec;
        adapter = new NatsKeyValueAdapter(connection, bucketName, kvo);
    }

    public EncodedKeyValueEntry<KeyType, DataType> get(KeyType key) throws IOException, JetStreamApiException {
        return _get(adapter.get(codec.encodeKey(key)));
    }

    public EncodedKeyValueEntry<KeyType, DataType> get(KeyType key, long revision) throws IOException, JetStreamApiException {
        return _get(adapter.get(codec.encodeKey(key), revision));
    }

    private EncodedKeyValueEntry<KeyType, DataType> _get(KeyValueEntry kve) {
        if (kve == null) {
            return null;
        }
        return new EncodedKeyValueEntry<>(kve, codec);
    }

    public List<EncodedKeyValueEntry<KeyType, DataType>> history(KeyType key) throws IOException, JetStreamApiException, InterruptedException {
        List<KeyValueEntry> entries = adapter.history(codec.encodeKey(key));
        List<EncodedKeyValueEntry<KeyType, DataType>> encodedEntries = new ArrayList<>();
        for (KeyValueEntry kve: entries) {
            encodedEntries.add(new EncodedKeyValueEntry<>(kve, codec));
        }
        return encodedEntries;
    }

    public long put(KeyType key, DataType value) throws IOException, JetStreamApiException {
        return adapter.put(codec.encodeKey(key), codec.encodeData(value));
    }

    public long create(KeyType key, DataType value) throws IOException, JetStreamApiException {
        return adapter.create(codec.encodeKey(key), codec.encodeData(value));
    }

    public long update(KeyType key, DataType value, long expectedRevision) throws IOException, JetStreamApiException {
        return adapter.update(codec.encodeKey(key), codec.encodeData(value), expectedRevision);
    }

    public void delete(KeyType key) throws IOException, JetStreamApiException {
        adapter.delete(codec.encodeKey(key));
    }

    public void delete(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        adapter.delete(codec.encodeKey(key), expectedRevision);
    }

    public void purge(KeyType key) throws IOException, JetStreamApiException {
        adapter.purge(codec.encodeKey(key));
    }

    public void purge(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        adapter.purge(codec.encodeKey(key), expectedRevision);
    }

    public LinkedBlockingQueue<EncodedKeyResult<KeyType, DataType>> consumeKeys() {
        return _consumeKeys(Collections.singletonList(adapter.readSubject(">")));
    }

    public LinkedBlockingQueue<EncodedKeyResult<KeyType, DataType>> consumeKeys(KeyType filter) {
        if (!codec.allowsFiltering()) {
            throw new UnsupportedOperationException("Filters not supported");
        }
        return _consumeKeys(Collections.singletonList(adapter.readSubject(codec.encodeFilter(filter))));
    }

    public LinkedBlockingQueue<EncodedKeyResult<KeyType, DataType>> consumeKeys(List<KeyType> filters) {
        if (!codec.allowsFiltering()) {
            throw new UnsupportedOperationException("Filters not supported");
        }
        List<String> encodedFilters = new ArrayList<>(filters.size());
        for(KeyType f : filters) {
            encodedFilters.add(adapter.readSubject(codec.encodeFilter(f)));
        }

        return this._consumeKeys(encodedFilters);
    }

    private LinkedBlockingQueue<EncodedKeyResult<KeyType, DataType>> _consumeKeys(List<String> readSubjectFilters) {
        LinkedBlockingQueue<EncodedKeyResult<KeyType, DataType>> q = new LinkedBlockingQueue<>();
        connection.getOptions().getExecutor().submit( () -> {
            try {
                adapter.visitSubject(readSubjectFilters, DeliverPolicy.LastPerSubject, true, false, m -> {
                    KeyValueOperation op = getOperation(m.getHeaders());
                    if (op == KeyValueOperation.PUT) {
                        q.offer(new EncodedKeyResult<>(
                            new KeyResult(
                                new NatsKeyValueUtil.BucketAndKey(m).key), codec));
                    }
                });
                q.offer(new EncodedKeyResult<>(new KeyResult(), codec));
            }
            catch (IOException | JetStreamApiException e) {
                q.offer(new EncodedKeyResult<>(new KeyResult(e), codec));
            }
            catch (InterruptedException e) {
                q.offer(new EncodedKeyResult<>(new KeyResult(e), codec));
                Thread.currentThread().interrupt();
            }
        });

        return q;
    }
}
