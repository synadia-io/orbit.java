// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import io.synadia.ekv.misc.Data;
import org.apache.commons.codec.binary.Base64;

public class DataKeyCodec implements KeyCodec<Data> {
    // It's fine to have an object as a value.
    // Base64 does the trick, but any two-way encoding is fine.
    Base64 base64 = new Base64();

    @Override
    public String encode(Data key) {
        return base64.encodeAsString(key.serialize());
    }

    @Override
    public Data decode(String encoded) {
        return new Data(base64.decode(encoded));
    }

    // This does not allow filtering, so we don't override these methods from the KeyCodec interface
//    default boolean allowsFiltering() { return false; }
//
//    default String encodeFilter(T filter) {
//        throw new UnsupportedOperationException("Filter encoding not supported");
//    }
}
