// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.support;

import io.synadia.ekv.ValueCodec;
import org.apache.commons.codec.binary.Base64;

import java.nio.charset.StandardCharsets;

public class TestStringValueCodec implements ValueCodec<String> {
    final Base64 base64;

    public TestStringValueCodec(boolean useBase64) {
        base64 = useBase64 ? new Base64() : null;
    }

    @Override
    public byte[] encode(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] encodedValue) {
        return encodedValue == null || encodedValue.length == 0 ? null : new String(encodedValue, StandardCharsets.UTF_8);
    }
}
