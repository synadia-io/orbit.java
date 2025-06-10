package io.synadia.ekv.support;

import io.synadia.ekv.ValueCodec;
import org.apache.commons.codec.binary.Base64;

public class DataValueCodec implements ValueCodec<Data> {
    // It's fine to have an object as a key. 
    // It needs to be encoded to a string that is a valid key
    // Base64 does the trick, but any two-way encoding is fine.
    final Base64 base64;

    public DataValueCodec(boolean useBase64) {
        base64 = useBase64 ? new Base64() : null;
    }

    @Override
    public byte[] encode(Data value) {
        if (value == null) {
            return null;
        }
        if (base64 == null) {
            return value.serialize();
        }
        return base64.encode(value.serialize());
    }

    @Override
    public Data decode(byte[] encodedValue) {
        if (encodedValue == null || encodedValue.length == 0) {
            return null;
        }
        if (base64 == null) {
            return new Data(encodedValue);
        }
        return new Data(base64.decode(encodedValue));
    }
}
