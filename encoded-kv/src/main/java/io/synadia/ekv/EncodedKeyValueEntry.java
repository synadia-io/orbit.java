// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;

import java.time.ZonedDateTime;

public class EncodedKeyValueEntry<KeyType, DataType> {
    final KeyValueEntry kve;
    final Codec<KeyType, DataType> codec;

    public EncodedKeyValueEntry(KeyValueEntry kve, Codec<KeyType, DataType> codec) {
        this.kve = kve;
        this.codec = codec;
    }

    public String getBucket() {
        return kve.getBucket();
    }

    public KeyType getKey() throws Exception {
        return codec.decodeKey(kve.getKey());
    }

    public DataType getValue() {
        return codec.decodeData(kve.getValue());
    }

    public long getDataLen() {
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
