package io.synadia.ekv.impl;

import io.synadia.ekv.KeyCodec;

import static io.nats.client.support.Validator.validateNonWildcardKvKeyRequired;

public abstract class StringKeyCodec implements KeyCodec<String> {

    protected String encodeSegment(String segment) {
        return segment;
    }

    protected String decodeSegment(String segment) {
        return segment;
    }

    @Override
    public String encode(String key) {
        String[] split = validateNonWildcardKvKeyRequired(key).split("\\.");
        StringBuilder sb = new StringBuilder(encodeSegment(split[0]));
        for (int i = 1; i < split.length; i++) {
            sb.append(".");
            sb.append(encodeSegment(split[i]));
        }
        return sb.toString();
    }

    @Override
    public String decode(String encoded) {
        String[] split = encoded.split("\\.");
        StringBuilder sb = new StringBuilder(decodeSegment(split[0]));
        for (int i = 1; i < split.length; i++) {
            sb.append(".");
            sb.append(decodeSegment(split[i]));
        }
        return sb.toString();
    }

    @Override
    public boolean allowsFiltering() {
        return true;
    }

    @Override
    public String encodeFilter(String filter) {
        String[] split = filter.split("\\.");
        StringBuilder sb = new StringBuilder(encodeSegmentIfNotWild(split[0]));
        for (int i = 1; i < split.length; i++) {
            sb.append(".");
            sb.append(encodeSegmentIfNotWild(split[i]));
        }
        return sb.toString();
    }

    private String encodeSegmentIfNotWild(String s) {
        return s.equals("*") || s.equals(">") ? s : encodeSegment(s);
    }
}
