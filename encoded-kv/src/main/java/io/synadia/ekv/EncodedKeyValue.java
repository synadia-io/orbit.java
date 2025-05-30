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
        kv = connection.keyValue(bucketName, kvo);
        this.codec = codec;
    }

    public EncodedKeyValue(KeyValue kv, Codec<KeyType, DataType> codec) {
        this.kv = kv;
        this.codec = codec;
    }

    public String getBucketName() {
        return kv.getBucketName();
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

    public List<KeyType> keys() throws IOException, JetStreamApiException, InterruptedException {
        return toKeyTypeList(kv.keys());
    }

    public List<KeyType> keys(KeyType filter) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return toKeyTypeList(kv.keys(codec.encodeFilter(filter)));
    }

    public List<KeyType> keys(List<KeyType> filters) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(filters.size());
        for(KeyType f : filters) {
            encodedFilters.add(codec.encodeFilter(f));
        }
        return toKeyTypeList(kv.keys(encodedFilters));
    }

    private List<KeyType> toKeyTypeList(List<String> stringKeys) {
        List<KeyType> ktl = new ArrayList<>(stringKeys.size());
        for(String s : stringKeys) {
            ktl.add(codec.decodeKey(s));
        }
        return ktl;
    }

    public EncodedKeyConsumer<KeyType, DataType> consumeKeys() {
        return new EncodedKeyConsumer<>(kv.consumeKeys(), codec);
    }

    public EncodedKeyConsumer<KeyType, DataType> consumeKeys(KeyType filter) {
        validateAllowsFiltering();
        return new EncodedKeyConsumer<>(kv.consumeKeys(codec.encodeFilter(filter)), codec);
    }

    public EncodedKeyConsumer<KeyType, DataType> consumeKeys(List<KeyType> filters) {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(filters.size());
        for(KeyType f : filters) {
            encodedFilters.add(codec.encodeFilter(f));
        }
        return new EncodedKeyConsumer<>(kv.consumeKeys(encodedFilters), codec);
    }

    class EkvWatcher implements KeyValueWatcher {
        final EncodedKeyValueWatcher<KeyType, DataType> userWatcher;

        public EkvWatcher(EncodedKeyValueWatcher<KeyType, DataType> userWatcher) {
            this.userWatcher = userWatcher;
        }

        @Override
        public void watch(KeyValueEntry kve) {
            userWatcher.watch(new EncodedKeyValueEntry<>(kve, codec));
        }

        @Override
        public void endOfData() {
            userWatcher.endOfData();
        }
    }

    public NatsKeyValueWatchSubscription watch(KeyType key, EncodedKeyValueWatcher<KeyType, DataType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return kv.watch(codec.encodeFilter(key), new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(KeyType key, EncodedKeyValueWatcher<KeyType, DataType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        return kv.watch(codec.encodeFilter(key), new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(List<KeyType> keys, EncodedKeyValueWatcher<KeyType, DataType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(keys.size());
        for(KeyType f : keys) {
            encodedFilters.add(codec.encodeFilter(f));
        }
        return kv.watch(encodedFilters, new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watch(List<KeyType> keys, EncodedKeyValueWatcher<KeyType, DataType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        validateAllowsFiltering();
        List<String> encodedFilters = new ArrayList<>(keys.size());
        for(KeyType f : keys) {
            encodedFilters.add(codec.encodeFilter(f));
        }
        return kv.watch(encodedFilters, new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    public NatsKeyValueWatchSubscription watchAll(EncodedKeyValueWatcher<KeyType, DataType> watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return kv.watchAll(new EkvWatcher(watcher), watchOptions);
    }

    public NatsKeyValueWatchSubscription watchAll(EncodedKeyValueWatcher<KeyType, DataType> watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return kv.watchAll(new EkvWatcher(watcher), fromRevision, watchOptions);
    }

    private void validateAllowsFiltering() {
        if (!codec.allowsFiltering()) {
            throw new UnsupportedOperationException("Filters not supported");
        }
    }
}
