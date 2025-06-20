// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.synadia.ekv.codec.DataValueCodec;
import io.synadia.ekv.codec.GeneralStringKeyCodec;
import io.synadia.ekv.codec.PathKeyCodec;
import io.synadia.ekv.codec.StringKeyCodec;
import io.synadia.ekv.misc.Data;
import io.synadia.ekv.misc.GeneralType;
import nats.io.NatsServerRunner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

public class CodecTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @Test
    public void testStringKeyCodec() {
        StringKeyCodec codec = new StringKeyCodec();
        assertTrue(codec.allowsFiltering());
        assertEquals("foo", codec.encode("foo"));
        assertEquals("foo.bar", codec.encode("foo.bar"));
        assertEquals("foo.bar", codec.encodeFilter("foo.bar"));
        assertEquals("foo.*", codec.encodeFilter("foo.*"));
        assertEquals("foo.>", codec.encodeFilter("foo.>"));

        assertEquals("foo", codec.decode("foo"));
        assertEquals("foo.bar", codec.decode("foo.bar"));

        DataValueCodec dvc = new DataValueCodec(GeneralType.PLAIN);
        Data value = new Data("data", "foo", false);
        byte[] jsonBytes = value.serialize();
        byte[] encoded = dvc.encode(value);
        assertArrayEquals(jsonBytes, encoded);

        Data decoded = dvc.decode(encoded);
        assertEquals(value, decoded);
    }

    @Test
    public void testEncodableStringKeyCodecBase64() {
        Base64 base64 = new Base64();
        String foo64 = base64.encodeToString("foo".getBytes());
        String fooBar64 = foo64 + "." + base64.encodeToString("bar".getBytes());
        String fooStar64 = foo64 + ".*";
        String fooGt64 = foo64 + ".>";

        GeneralStringKeyCodec codec = new GeneralStringKeyCodec(GeneralType.BASE64);
        assertTrue(codec.allowsFiltering());
        assertEquals(foo64, codec.encode("foo"));
        assertEquals(fooBar64, codec.encode("foo.bar"));
        assertEquals(fooBar64, codec.encodeFilter("foo.bar"));
        assertEquals(fooStar64, codec.encodeFilter("foo.*"));
        assertEquals(fooGt64, codec.encodeFilter("foo.>"));

        assertEquals("foo", codec.decode(foo64));
        assertEquals("foo.bar", codec.decode(fooBar64));

        DataValueCodec dvc = new DataValueCodec(GeneralType.BASE64);
        Data value = new Data("data", "foo", false);
        byte[] jsonBytes = value.serialize();
        byte[] encoded64 = base64.encode(jsonBytes);
        byte[] encoded = dvc.encode(value);
        assertArrayEquals(encoded64, encoded);

        Data decoded = dvc.decode(encoded);
        assertEquals(value, decoded);
    }

    @Test
    public void testEncodableStringKeyCodecHex() throws Exception {
        Hex hex = new Hex();
        String fooHex = toHexString(hex, "foo");
        String fooBarHex = fooHex + "." + toHexString(hex, "bar");
        String fooStarHex = fooHex + ".*";
        String fooGtHex = fooHex + ".>";

        GeneralStringKeyCodec codec = new GeneralStringKeyCodec(GeneralType.HEX);
        assertTrue(codec.allowsFiltering());
        assertEquals(fooHex, codec.encode("foo"));
        assertEquals(fooBarHex, codec.encode("foo.bar"));
        assertEquals(fooBarHex, codec.encodeFilter("foo.bar"));
        assertEquals(fooStarHex, codec.encodeFilter("foo.*"));
        assertEquals(fooGtHex, codec.encodeFilter("foo.>"));

        assertEquals("foo", codec.decode(fooHex));
        assertEquals("foo.bar", codec.decode(fooBarHex));

        DataValueCodec dvc = new DataValueCodec(GeneralType.HEX);
        Data value = new Data("data", "foo", false);
        byte[] jsonBytes = value.serialize();
        byte[] encoded64 = hex.encode(jsonBytes);
        byte[] encoded = dvc.encode(value);
        assertArrayEquals(encoded64, encoded);

        Data decoded = dvc.decode(encoded);
        assertEquals(value, decoded);
    }

    private static String toHexString(Hex hex, String s) {
        return new String(hex.encode(s.getBytes()), StandardCharsets.US_ASCII);
    }

    @Test
    public void testPathCodec() {
        validatePathCodec("/foo/bar", "_root_.foo.bar", "/foo/bar", false); // simple path
        validatePathCodec("foo/bar", "foo.bar", "foo/bar", false); // no leading slash
        validatePathCodec("/foo/bar/baz/qux", "_root_.foo.bar.baz.qux", "/foo/bar/baz/qux", false); // deep path
        validatePathCodec("/foo", "_root_.foo", "/foo", false); // single segment
        validatePathCodec("foo/bar/", "foo.bar", "foo/bar", true); // trailing slash
        validatePathCodec("/", "_root_", "/", false); // root only
        validatePathCodec("/foo/bar/", "_root_.foo.bar", "/foo/bar", true); // leading slash with trailing
    }

    private void validatePathCodec(String path, String encoded, String decoded, boolean trailingSlash) {
        PathKeyCodec pkcNoTrailingPath1 = new PathKeyCodec();
        PathKeyCodec pkcNoTrailingPath2 = new PathKeyCodec(false);

        assertEquals(encoded, pkcNoTrailingPath1.encode(path));
        assertEquals(encoded, pkcNoTrailingPath2.encode(path));

        assertEquals(decoded, pkcNoTrailingPath1.decode(encoded));
        assertEquals(decoded, pkcNoTrailingPath2.decode(encoded));

        if (trailingSlash) {
            PathKeyCodec pkcTrailingPath = new PathKeyCodec(true);
            assertEquals(encoded + PathKeyCodec.TRAILING_SUFFIX, pkcTrailingPath.encode(path));
            assertEquals(path, pkcTrailingPath.decode(decoded + PathKeyCodec.TRAILING_SUFFIX));
        }
    }
}
