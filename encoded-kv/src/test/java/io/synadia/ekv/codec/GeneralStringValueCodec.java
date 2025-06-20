// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import io.synadia.ekv.misc.GeneralType;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;

public class GeneralStringValueCodec implements ValueCodec<String> {
    final GeneralType gt;

    public GeneralStringValueCodec(GeneralType gt) {
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
                return new String(Hex.encodeHex(bytes)).getBytes(StandardCharsets.UTF_8);
        }

        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String decode(byte[] encodedValue) {
        if (encodedValue == null || encodedValue.length == 0) {
            return null;
        }

        switch (gt) {
            case BASE64:
                byte[] decodedBytes = Base64.decodeBase64(encodedValue);
                return new String(decodedBytes, StandardCharsets.UTF_8);
            case HEX:
                try {
                    byte[] hexBytes = Hex.decodeHex(new String(encodedValue, StandardCharsets.UTF_8));
                    return new String(hexBytes, StandardCharsets.UTF_8);
                }
                catch (DecoderException e) {
                    throw new RuntimeException(e);
                }
        }
        return new String(encodedValue, StandardCharsets.UTF_8);
    }
}
