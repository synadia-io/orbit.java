// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import java.util.Arrays;
import java.util.List;

public class ChainedValueCodec<ValueType> implements ValueCodec<ValueType> {

    private final ValueCodec<ValueType> mainCodec;
    private final List<ValueCodec<byte[]>> chain;

    public ChainedValueCodec(ValueCodec<ValueType> mainCodec, List<ValueCodec<byte[]>> chain) {
        this.mainCodec = mainCodec;
        this.chain = chain;
    }

    @SafeVarargs
    public ChainedValueCodec(ValueCodec<ValueType> mainCodec, ValueCodec<byte[]>... chain) {
        this(mainCodec, Arrays.asList(chain));
    }

    @Override
    public byte[] encode(ValueType value) {
        byte[] encoded = mainCodec.encode(value);
        for (ValueCodec<byte[]> codec : chain) {
            encoded = codec.encode(encoded);
        }
        return encoded;
    }

    @Override
    public ValueType decode(byte[] encodedValue) {
        byte[] decoded = encodedValue;
        for (int i = chain.size() - 1; i >= 0; i--) {
            decoded = chain.get(i).decode(decoded);
        }
        return mainCodec.decode(decoded);
    }
}
