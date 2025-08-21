// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueOptions;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.KeyValueWatcher;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.synadia.ekv.codec.KeyCodec;
import io.synadia.ekv.codec.ValueCodec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KvEncoded<KeyType, ValueType> {
    private final KeyValue kv;
    private final KeyCodec<KeyType> keyCodec;
    private final ValueCodec<ValueType> valueCodec;

    public KvEncoded(Connection connection, String bucketName, KeyCodec<KeyType> keyCodec, ValueCodec<ValueType> valueCodec) throws IOException {
        this(connection, bucketName, keyCodec, valueCodec, null);
    }

    public KvEncoded(Connection connection, String bucketName, KeyCodec<KeyType> keyCodec, ValueCodec<ValueType> valueCodec, KeyValueOptions kvo) throws IOException {
        kv = connection.keyValue(bucketName, kvo);
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public KvEncoded(KeyValue kv, KeyCodec<KeyType> keyCodec, ValueCodec<ValueType> valueCodec) {
        this.kv = kv;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public String getBucketName() {
        return kv.getBucketName();
    }

    public EncodedKeyValueEntry<KeyType, ValueType> get(KeyType key) throws IOException, JetStreamApiException {
        return _get(kv.get(keyCodec.encode(key)));
    }

    public EncodedKeyValueEntry<KeyType, ValueType> get(KeyType key, long revision) throws IOException, JetStreamApiException {
        return _get(kv.get(keyCodec.encode(key), revision));
    }

    private EncodedKeyValueEntry<KeyType, ValueType> _get(KeyValueEntry kve) {
        if (kve == null) {
            return null;
        }
        return new EncodedKeyValueEntry<>(kve, keyCodec, valueCodec);
    }

    public List<EncodedKeyValueEntry<KeyType, ValueType>> history(KeyType key) throws IOException, JetStreamApiException, InterruptedException {
        List<KeyValueEntry> entries = kv.history(keyCodec.encode(key));
        List<EncodedKeyValueEntry<KeyType, ValueType>> encodedEntries = new ArrayList<>();
        for (KeyValueEntry kve: entries) {
            encodedEntries.add(new EncodedKeyValueEntry<>(kve, keyCodec, valueCodec));
        }
        return encodedEntries;
    }

    public long put(KeyType key, ValueType value) throws IOException, JetStreamApiException {
        return kv.put(keyCodec.encode(key), valueCodec.encode(value));
    }

    public long create(KeyType key, ValueType value) throws IOException, JetStreamApiException {
        return kv.create(keyCodec.encode(key), valueCodec.encode(value));
    }

    public long update(KeyType key, ValueType value, long expectedRevision) throws IOException, JetStreamApiException {
        return kv.update(keyCodec.encode(key), valueCodec.encode(value), expectedRevision);
    }

    public void delete(KeyType key) throws IOException, JetStreamApiException {
        kv.delete(keyCodec.encode(key));
    }

    public void delete(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        kv.delete(keyCodec.encode(key), expectedRevision);
    }

    public void purge(KeyType key) throws IOException, JetStreamApiException {
        kv.purge(keyCodec.encode(key));
    }

    public void purge(KeyType key, long expectedRevision) throws IOException, JetStreamApiException {
        kv.purge(keyCodec.encode(key), expectedRevision);
    }

    public List<KeyType> keys() throws IOException, JetStreamApiException, InterruptedException {
        return toKeyTypeList(kv.keys());
    }

    public List<KeyType> keys(KeyType filter) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return toKeyTypeList(kv.keys(keyCodec.encodeFilter(filter)));
    }

    public List<KeyType> keys(List<KeyType> filters) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(filters.size());
        for(KeyType f : filters) {
            encodedFilters.add(keyCodec.encodeFilter(f));
        }
        return toKeyTypeList(kv.keys(encodedFilters));
    }

    private List<KeyType> toKeyTypeList(List<String> stringKeys) {
        List<KeyType> ktl = new ArrayList<>(stringKeys.size());
        for(String s : stringKeys) {
            ktl.add(keyCodec.decode(s));
        }
        return ktl;
    }

    public EncodedKeyConsumer<KeyType> consumeKeys() {
        return new EncodedKeyConsumer<>(kv.consumeKeys(), keyCodec);
    }

    public EncodedKeyConsumer<KeyType> consumeKeys(KeyType filter) {
        validateAllowsFiltering();
        return new EncodedKeyConsumer<>(kv.consumeKeys(keyCodec.encodeFilter(filter)), keyCodec);
    }

    public EncodedKeyConsumer<KeyType> consumeKeys(List<KeyType> filters) {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(filters.size());
        for(KeyType f : filters) {
            encodedFilters.add(keyCodec.encodeFilter(f));
        }
        return new EncodedKeyConsumer<>(kv.consumeKeys(encodedFilters), keyCodec);
    }

    class EkvWatcher implements KeyValueWatcher {
        final EncodedKeyValueWatcher<KeyType, ValueType> userWatcher;

        public EkvWatcher(EncodedKeyValueWatcher<KeyType, ValueType> userWatcher) {
            this.userWatcher = userWatcher;
        }

        @Override
        public void watch(KeyValueEntry kve) {
            userWatcher.watch(new EncodedKeyValueEntry<>(kve, keyCodec, valueCodec));
        }

        @Override
        public void endOfData() {
            userWatcher.endOfData();
        }
    }

    public NatsKeyValueWatchSubscription watch(KeyType key, EncodedKeyValueWatcher<KeyType, ValueType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return kv.watch(keyCodec.encodeFilter(key), new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(KeyType key, EncodedKeyValueWatcher<KeyType, ValueType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return kv.watch(keyCodec.encodeFilter(key), new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(List<KeyType> keys, EncodedKeyValueWatcher<KeyType, ValueType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(keys.size());
        for(KeyType f : keys) {
            encodedFilters.add(keyCodec.encodeFilter(f));
        }
        return kv.watch(encodedFilters, new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(List<KeyType> keys, EncodedKeyValueWatcher<KeyType, ValueType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(keys.size());
        for(KeyType f : keys) {
            encodedFilters.add(keyCodec.encodeFilter(f));
        }
        return kv.watch(encodedFilters, new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    public NatsKeyValueWatchSubscription watchAll(EncodedKeyValueWatcher<KeyType, ValueType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return kv.watchAll(new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watchAll(EncodedKeyValueWatcher<KeyType, ValueType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return kv.watchAll(new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    private void validateAllowsFiltering() {
        if (!keyCodec.allowsFiltering()) {
            throw new UnsupportedOperationException("Filters not supported");
        }
    }
}
