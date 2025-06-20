// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import io.synadia.ekv.misc.Data;
import io.synadia.ekv.misc.GeneralType;

public class DataValueCodec implements ValueCodec<Data> {
    // It's fine to have an object as a key. 
    // It needs to be encoded to a string that is a valid key
    // Base64 does the trick, but any two-way encoding is fine.
    final GeneralByteValueCodec gvc;

    public DataValueCodec(GeneralType gt) {
        gvc = new GeneralByteValueCodec(gt);
    }

    @Override
    public byte[] encode(Data value) {
        if (value == null) {
            return null;
        }
        return gvc.encode(value.serialize());
    }

    @Override
    public Data decode(byte[] encodedValue) {
        if (encodedValue == null || encodedValue.length == 0) {
            return null;
        }
        return new Data(gvc.decode(encodedValue));
    }
}
