// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

public interface KeyCodec<T> {
    String encode(T key);
    T decode(String encoded);

    default boolean allowsFiltering() { return false; }

    default String encodeFilter(T filter) {
        throw new UnsupportedOperationException("Filter encoding not supported");
    }
}
