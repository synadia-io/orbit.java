// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.synadia.ekv.codec.AbstractEncodableStringKeyCodec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;

public class GeneralKeyCodec extends AbstractEncodableStringKeyCodec {
    final GeneralType gt;

    public GeneralKeyCodec(GeneralType gt) {
        this.gt = gt;
    }

    @Override
    protected String encodeSegment(String segment) {
        switch (gt) {
            case BASE64:
                return Base64.encodeBase64String(segment.getBytes(StandardCharsets.UTF_8));
            case HEX:
                return new String(Hex.encodeHex(segment.getBytes(StandardCharsets.UTF_8)));
        }
        return segment;
    }

    @Override
    protected String decodeSegment(String encoded) {
        switch (gt) {
            case BASE64:
                return new String(Base64.decodeBase64(encoded.getBytes(StandardCharsets.US_ASCII)));
            case HEX:
                try {
                    return new String(Hex.decodeHex(encoded), StandardCharsets.UTF_8);
                }
                catch (DecoderException e) {
                    throw new RuntimeException(e);
                }

        }
        return encoded;
    }
}
