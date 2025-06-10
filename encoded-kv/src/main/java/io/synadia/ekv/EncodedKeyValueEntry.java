// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.synadia.ekv.codec.KeyCodec;
import io.synadia.ekv.codec.ValueCodec;

import java.time.ZonedDateTime;

public class EncodedKeyValueEntry<KeyType, ValueType> {
    private final KeyValueEntry kve;
    private final KeyCodec<KeyType> keyCodec;
    private final ValueCodec<ValueType> valueCodec;

    public EncodedKeyValueEntry(KeyValueEntry kve, KeyCodec<KeyType> keyCodec, ValueCodec<ValueType> valueCodec) {
        this.kve = kve;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    public String getBucket() {
        return kve.getBucket();
    }

    public KeyType getKey() {
        return keyCodec.decode(kve.getKey());
    }

    public ValueType getValue() {
        return valueCodec.decode(kve.getValue());
    }

    public long getEncodedDataLen() {
        return kve.getDataLen();
    }

    public ZonedDateTime getCreated() {
        return kve.getCreated();
    }

    public long getRevision() {
        return kve.getRevision();
    }

    public long getDelta() {
        return kve.getDelta();
    }

    public KeyValueOperation getOperation() {
        return kve.getOperation();
    }
}
