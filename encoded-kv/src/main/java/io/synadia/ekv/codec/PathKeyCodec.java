// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

public class PathKeyCodec implements KeyCodec<String> {

    public static final String ROOT_PREFIX = "_root_";
    public static final String TRAILING_SUFFIX = "_trail_";
    private static final int ROOT_PREFIX_LEN = ROOT_PREFIX.length();
    private static final String FWD_SLASH = "/";

    private final boolean keepTrailingSlash;

    public PathKeyCodec() {
        this.keepTrailingSlash = false;
    }

    public PathKeyCodec(boolean keepTrailingSlash) {
        this.keepTrailingSlash = keepTrailingSlash;
    }

    @Override
    public String encode(String key) {
        if (key.startsWith(FWD_SLASH)) {
            if (key.length() == 1) {
                return ROOT_PREFIX;
            }
            key = ROOT_PREFIX + "." + key.substring(1);
        }
        if (key.endsWith(FWD_SLASH)) {
            key = key.substring(0, key.length() - 1);
            if (keepTrailingSlash) {
                key = key + TRAILING_SUFFIX;
            }
        }
        return key.replace('/', '.');
    }

    @Override
    public String decode(String encoded) {
        if (encoded.startsWith(ROOT_PREFIX)) {
            if (encoded.length() == ROOT_PREFIX_LEN) {
                return FWD_SLASH;
            }
            return encoded.substring(ROOT_PREFIX_LEN).replace('.', '/');
        }
        return encoded.replace('.', '/').replace(TRAILING_SUFFIX, "/");
    }
}
