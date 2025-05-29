package io.synadia.kv.codec;

import org.apache.commons.codec.binary.Base64;

public class KeyOrValueCodec implements Codec<KeyOrValue, KeyOrValue> {
    Base64 base64 = new Base64();

    @Override
    public String encodeKey(KeyOrValue key) {
        return base64.encodeAsString(key.serialize());
    }

    @Override
    public byte[] encodeData(KeyOrValue value) {
        return value == null ? null : value.serialize();
    }

    @Override
    public KeyOrValue decodeKey(String key) throws Exception {
        return new KeyOrValue(base64.decode(key));
    }

    @Override
    public KeyOrValue decodeData(byte[] data) throws Exception {
        return data == null || data.length == 0 ? null : new KeyOrValue(data);
    }
}
