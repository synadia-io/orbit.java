package io.synadia.kv.codec;

import static io.nats.client.support.Validator.validateNonWildcardKvKeyRequired;

public abstract class StringKeyCodec<DataType> implements Codec<String, DataType> {
    @Override
    public String encodeKey(String key) {
        return validateNonWildcardKvKeyRequired(key);
    }

    @Override
    public boolean allowsFiltering() {
        return true;
    }

    @Override
    public String encodeFilter(String filter) {
        return filter;
    }

    @Override
    public String decodeKey(String key) throws Exception {
        return key;
    }
}
