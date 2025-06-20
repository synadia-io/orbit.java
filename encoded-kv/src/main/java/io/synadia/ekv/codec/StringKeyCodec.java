// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

public class StringKeyCodec implements KeyCodec<String> {

    @Override
    public String encode(String key) {
        return key;
    }

    @Override
    public String decode(String encoded) {
        return encoded;
    }

    @Override
    public boolean allowsFiltering() {
        return true;
    }

    @Override
    public String encodeFilter(String filter) {
        return filter;
    }
}
