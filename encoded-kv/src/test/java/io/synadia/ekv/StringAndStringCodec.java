// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import org.apache.commons.codec.binary.Base64;

import java.nio.charset.StandardCharsets;

public class StringAndStringCodec extends StringKeyCodec<String> {
    final Base64 base64;

    public StringAndStringCodec(boolean useBase64) {
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
    public byte[] encodeData(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decodeData(byte[] data) {
        return data == null || data.length == 0 ? null : new String(data, StandardCharsets.UTF_8);
    }
}
