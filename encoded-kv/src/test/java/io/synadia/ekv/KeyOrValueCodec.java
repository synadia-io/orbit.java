package io.synadia.ekv;

import org.apache.commons.codec.binary.Base64;

public class KeyOrValueCodec implements Codec<KeyOrValue, KeyOrValue> {
    // It's fine to have an object as a key. 
    // It needs to be encoded to a string that is a valid key
    // Base64 does the trick, but any two-way encoding is fine.
    Base64 encoder = new Base64();

    @Override
    public String encodeKey(KeyOrValue key) {
        return encoder.encodeAsString(key.serialize());
    }

    @Override
    public byte[] encodeData(KeyOrValue value) {
        return value == null ? null : value.serialize();
    }

    @Override
    public KeyOrValue decodeKey(String key) {
        return new KeyOrValue(encoder.decode(key));
    }

    @Override
    public KeyOrValue decodeData(byte[] data) {
        return data == null || data.length == 0 ? null : new KeyOrValue(data);
    }
}
