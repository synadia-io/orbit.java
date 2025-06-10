package io.synadia.ekv;

import io.synadia.ekv.codec.ValueCodec;

import java.nio.charset.StandardCharsets;

public class DataValueCodec implements ValueCodec<Data> {
    // It's fine to have an object as a key. 
    // It needs to be encoded to a string that is a valid key
    // Base64 does the trick, but any two-way encoding is fine.
    final GeneralValueCodec gvc;

    public DataValueCodec(GeneralType gt) {
        gvc = new GeneralValueCodec(gt);
    }

    @Override
    public byte[] encode(Data value) {
        if (value == null) {
            return null;
        }
        return gvc.encode(value.toJson());
    }

    @Override
    public Data decode(byte[] encodedValue) {
        if (encodedValue == null || encodedValue.length == 0) {
            return null;
        }
        return new Data(gvc.decode(encodedValue).getBytes(StandardCharsets.UTF_8));
    }
}
