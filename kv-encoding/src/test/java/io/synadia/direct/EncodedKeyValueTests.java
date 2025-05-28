package io.synadia.direct;

import io.nats.client.Connection;
import io.nats.client.KeyValueManagement;
import io.nats.client.NUID;
import io.nats.client.Nats;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.support.*;
import io.synadia.kv.Codec;
import io.synadia.kv.EncodedKeyValue;
import io.synadia.kv.EncodedKeyValueEntry;
import nats.io.NatsServerRunner;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EncodedKeyValueTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    static class KeyOrValue implements JsonSerializable {
        public final String part1;
        public final String part2;
        public final boolean isKey;

        public KeyOrValue(String part1, String part2, boolean isKey) {
            this.part1 = part1;
            this.part2 = part2;
            this.isKey = isKey;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof KeyOrValue)) return false;

            KeyOrValue that = (KeyOrValue) o;
            return isKey == that.isKey && Objects.equals(part1, that.part1) && Objects.equals(part2, that.part2);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(part1);
            result = 31 * result + Objects.hashCode(part2);
            result = 31 * result + Boolean.hashCode(isKey);
            return result;
        }

        public KeyOrValue(String json) throws JsonParseException {
            this(json.getBytes(StandardCharsets.UTF_8));
        }

        private KeyOrValue(byte[] jsonBytes) throws JsonParseException {
            System.out.println("jsonBytes: " + new String(jsonBytes, StandardCharsets.UTF_8));
            JsonValue jv = JsonParser.parse(jsonBytes);
            this.isKey = JsonValueUtils.readBoolean(jv, "isKey", false);
            if (isKey) {
                part1 = JsonValueUtils.readString(jv, "k1");
                part2 = JsonValueUtils.readString(jv, "k2");
            }
            else {
                part1 = JsonValueUtils.readString(jv, "v1");
                part2 = JsonValueUtils.readString(jv, "v2");
            }
        }

        @Override
        public String toJson() {
            if (isKey) {
                return JsonValueUtils.mapBuilder().put("isKey", true).put("k1", part1).put("k2", part2).toJson();
            }
            return JsonValueUtils.mapBuilder().put("v1", part1).put("v2", part2).toJson();
        }

        @Override
        public String toString() {
            return toJson();
        }
    }

    static class TestCodec implements Codec<KeyOrValue, KeyOrValue> {
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

    @Test
    public void testDc() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                Codec<KeyOrValue, KeyOrValue> codec = new TestCodec();

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                EncodedKeyValue<KeyOrValue, KeyOrValue> ekv
                    = new EncodedKeyValue<>(nc, bucketName, codec);

                KeyOrValue key = new KeyOrValue("foo", null, true);
                KeyOrValue value = new KeyOrValue("bar", "baz", false);

                long revision = ekv.put(key, value);
                assertEquals(1, revision);

                EncodedKeyValueEntry<KeyOrValue, KeyOrValue> entry = ekv.get(key);
                assertNotNull(entry);
                assertEquals(key, entry.getKey());
                assertEquals(value, entry.getValue());
            }
        }
    }
}
