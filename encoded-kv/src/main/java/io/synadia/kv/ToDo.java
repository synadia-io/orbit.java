// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

import io.nats.client.JetStreamApiException;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.KeyValueWatcher;
import io.nats.client.impl.NatsKeyValueWatchSubscription;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class ToDo<KeyType, DataType> {

    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return null;
    }

    public List<String> keys() throws IOException, JetStreamApiException, InterruptedException {
        return Collections.emptyList();
    }

    public List<String> keys(String filter) throws IOException, JetStreamApiException, InterruptedException {
        return Collections.emptyList();
    }

    public List<String> keys(List<String> filters) throws IOException, JetStreamApiException, InterruptedException {
        return Collections.emptyList();
    }
}
