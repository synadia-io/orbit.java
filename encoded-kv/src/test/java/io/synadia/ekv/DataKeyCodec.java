package io.synadia.ekv;

import io.synadia.ekv.codec.KeyCodec;
import org.apache.commons.codec.binary.Base64;

public class DataKeyCodec implements KeyCodec<Data> {
    // It's fine to have an object as a value.
    // Base64 does the trick, but any two-way encoding is fine.
    Base64 base64 = new Base64();

    @Override
    public String encode(Data key) {
        return base64.encodeAsString(key.serialize());
    }

    @Override
    public Data decode(String encoded) {
        return new Data(base64.decode(encoded));
    }
}
