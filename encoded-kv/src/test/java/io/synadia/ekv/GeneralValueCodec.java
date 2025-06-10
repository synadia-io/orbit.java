// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.synadia.ekv.codec.ValueCodec;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;

public class GeneralValueCodec implements ValueCodec<String> {
    final GeneralType gt;

    public GeneralValueCodec(GeneralType gt) {
        this.gt = gt;
    }

    @Override
    public byte[] encode(String value) {
        if (value == null) {
            return null;
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        switch (gt) {
            case BASE64:
                return Base64.encodeBase64(bytes);
            case HEX:
                return new String(Hex.encodeHex(bytes)).getBytes(StandardCharsets.US_ASCII);
        }
        return bytes;
    }

    @Override
    public String decode(byte[] encodedValue) {
        if (encodedValue == null || encodedValue.length == 0) {
            return null;
        }

        byte[] decodedBytes = encodedValue;
        switch (gt) {
            case BASE64:
                decodedBytes = Base64.decodeBase64(encodedValue);
                break;
            case HEX:
                try {
                    decodedBytes = Hex.decodeHex(new String(encodedValue, StandardCharsets.US_ASCII));
                }
                catch (DecoderException e) {
                    throw new RuntimeException(e);
                }
                break;
        }
        return new String(decodedBytes, StandardCharsets.UTF_8);
    }
}
