// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import org.apache.commons.codec.binary.Base64;

public class StringAndKeyOrValueCodec extends StringKeyCodec<KeyOrValue> {
    final Base64 base64;

    public StringAndKeyOrValueCodec(boolean useBase64) {
        base64 = useBase64 ? new Base64() : null;
    }

    @Override
    protected String encodeSegment(String segment) {
        return base64 == null ? super.encodeSegment(segment) : base64.encodeToString(segment.getBytes());
    }

    @Override
    protected String decodeSegment(String segment) {
        return base64 == null ? super.decodeSegment(segment) : new String(base64.decode(segment));
    }

    @Override
    public byte[] encodeData(KeyOrValue value) {
        if (value == null) {
            return null;
        }
        if (base64 == null) {
            return value.serialize();
        }
        return base64.encode(value.serialize());
    }

    @Override
    public KeyOrValue decodeData(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        if (base64 == null) {
            return new KeyOrValue(data);
        }
        return new KeyOrValue(base64.decode(data));
    }
}
