// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.support;

import io.synadia.ekv.impl.StringKeyCodec;
import org.apache.commons.codec.binary.Base64;

public class TestStringKeyCodec extends StringKeyCodec {
    final Base64 base64;

    public TestStringKeyCodec(boolean useBase64) {
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
}
