// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import java.util.Arrays;
import java.util.List;

public class ChainedKeyCodec<KeyType> implements KeyCodec<KeyType> {

    private final KeyCodec<KeyType> mainCodec;
    private final List<KeyCodec<String>> chain;
    private final boolean allowsFiltering;

    public ChainedKeyCodec(KeyCodec<KeyType> mainCodec, List<KeyCodec<String>> chain) {
        this.mainCodec = mainCodec;
        this.chain = chain;
        boolean b = mainCodec.allowsFiltering();
        if (b) {
            for (KeyCodec<String> codec : chain) {
                if (!codec.allowsFiltering()) {
                    b = false;
                    break;
                }
            }
        }
        allowsFiltering = b;
    }

    @SafeVarargs
    public ChainedKeyCodec(KeyCodec<KeyType> mainCodec, KeyCodec<String>... chain) {
        this(mainCodec, Arrays.asList(chain));
    }


    @Override
    public String encode(KeyType value) {
        String encoded = mainCodec.encode(value);
        for (KeyCodec<String> codec : chain) {
            encoded = codec.encode(encoded);
        }
        return encoded;
    }


    @Override
    public KeyType decode(String encoded) {
        String decoded = encoded;
        for (int i = chain.size() - 1; i >= 0; i--) {
            decoded = chain.get(i).decode(decoded);
        }
        return mainCodec.decode(decoded);
    }


    @Override
    public boolean allowsFiltering() {
        return allowsFiltering;
    }

    @Override
    public String encodeFilter(KeyType filter) {
        if (allowsFiltering) {
            String encoded = mainCodec.encodeFilter(filter);
            for (KeyCodec<String> codec : chain) {
                encoded = codec.encodeFilter(encoded);
            }
            return encoded;
        }
        throw new UnsupportedOperationException("Filter encoding not supported");
    }
}
