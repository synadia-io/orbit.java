// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv.codec;

import io.synadia.ekv.misc.GeneralType;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;

public class GeneralByteValueCodec implements ValueCodec<byte[]> {
    final GeneralType gt;

    public GeneralByteValueCodec(GeneralType gt) {
        this.gt = gt;
    }

    @Override
    public byte[] encode(byte[] value) {
        if (value != null) {
            switch (gt) {
                case BASE64:
                    return Base64.encodeBase64(value);
                case HEX:
                    return new String(Hex.encodeHex(value)).getBytes(StandardCharsets.US_ASCII);
            }
        }
        return value;
    }

    @Override
    public byte[] decode(byte[] encodedValue) {
        if (encodedValue != null && encodedValue.length > 0) {
            switch (gt) {
                case BASE64: return Base64.decodeBase64(encodedValue);
                case HEX:
                    try {
                        return Hex.decodeHex(new String(encodedValue, StandardCharsets.US_ASCII));
                    }
                    catch (DecoderException e) {
                        throw new RuntimeException(e);
                    }
            }
        }
        return encodedValue;
    }
}
