// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.StorageType;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import nats.io.NatsServerRunner;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.nats.client.api.KeyValueWatchOption.*;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class EncodedKeyValueTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @Test
    public void testStringKeyCodec() throws Exception {
        StringAndKeyOrValueCodec codec = new StringAndKeyOrValueCodec(false);
        assertTrue(codec.allowsFiltering());
        assertEquals("foo", codec.encodeKey("foo"));
        assertEquals("foo.bar", codec.encodeKey("foo.bar"));
        assertEquals("foo.bar", codec.encodeFilter("foo.bar"));
        assertEquals("foo.*", codec.encodeFilter("foo.*"));
        assertEquals("foo.>", codec.encodeFilter("foo.>"));

        assertEquals("foo", codec.decodeKey("foo"));
        assertEquals("foo.bar", codec.decodeKey("foo.bar"));

        KeyOrValue value = new KeyOrValue("data", "foo", false);
        byte[] jsonBytes = value.serialize();
        byte[] encoded = codec.encodeData(value);
        assertArrayEquals(jsonBytes, encoded);

        KeyOrValue decoded = codec.decodeData(encoded);
        assertEquals(value, decoded);
    }

    @Test
    public void testStringKeyCodecBase64() throws Exception {
        Base64 base64 = new Base64();
        String foo64 = base64.encodeToString("foo".getBytes());
        String fooBar64 = foo64 + "." + base64.encodeToString("bar".getBytes());
        String fooStar64 = foo64 + ".*";
        String fooGt64 = foo64 + ".>";

        StringAndKeyOrValueCodec codec = new StringAndKeyOrValueCodec(true);
        assertTrue(codec.allowsFiltering());
        assertEquals(foo64, codec.encodeKey("foo"));
        assertEquals(fooBar64, codec.encodeKey("foo.bar"));
        assertEquals(fooBar64, codec.encodeFilter("foo.bar"));
        assertEquals(fooStar64, codec.encodeFilter("foo.*"));
        assertEquals(fooGt64, codec.encodeFilter("foo.>"));

        assertEquals("foo", codec.decodeKey(foo64));
        assertEquals("foo.bar", codec.decodeKey(fooBar64));

        KeyOrValue value = new KeyOrValue("data", "foo", false);
        byte[] jsonBytes = value.serialize();
        byte[] encoded64 = base64.encode(jsonBytes);
        byte[] encoded = codec.encodeData(value);
        assertArrayEquals(encoded64, encoded);

        KeyOrValue decoded = codec.decodeData(encoded);
        assertEquals(value, decoded);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testStringKeyWorkflow(boolean useBase64) throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                StringAndKeyOrValueCodec codec = new StringAndKeyOrValueCodec(useBase64);

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                // this is just for coverage of constructors.
                EncodedKeyValue<String, KeyOrValue> ekv;
                if (useBase64) {
                    ekv = new EncodedKeyValue<>(nc, bucketName, codec);
                }
                else {
                    KeyValue kv = nc.keyValue(bucketName);
                    ekv = new EncodedKeyValue<>(kv, codec);
                }

                String key1 = "key.1";
                String key2 = "key.2";
                List<String> keyList = new ArrayList<>();
                keyList.add(key1);
                keyList.add(key2);

                KeyOrValue v1 = new KeyOrValue("v1", "foo", false);
                KeyOrValue v2 = new KeyOrValue("v2", "bar", false);

                validatePutRevision(1, ekv.put(key1, v1));
                validatePutRevision(2, ekv.put(key2, v2));

                validateGet(key1, v1, ekv.get(key1));
                validateGet(key2, v2, ekv.get(key2));

                assertNull(ekv.get("not-found"));

                validateKeys(key1, key2, ekv.keys());
                validateKeys(key1, key2, ekv.keys("key.*"));
                validateKeys(key1, null, ekv.keys(key1));
                validateKeys(null, key2, ekv.keys(key2));
                validateKeys(key1, key2, ekv.keys(keyList));

                validateKeys(key1, key2, getFromQueue(ekv.consumeKeys()));
                validateKeys(key1, key2, getFromQueue(ekv.consumeKeys("key.*")));
                validateKeys(key1, null, getFromQueue(ekv.consumeKeys(key1)));
                validateKeys(null, key2, getFromQueue(ekv.consumeKeys(key2)));
                validateKeys(key1, key2, getFromQueue(ekv.consumeKeys(keyList)));

                String stream = "KV_" + bucketName;
                JetStreamSubscription sub = nc.jetStream().subscribe(">", PushSubscribeOptions.builder().stream(stream).build());
                Message m1 = sub.nextMessage(Duration.ofSeconds(1));
                Message m2 = sub.nextMessage(Duration.ofSeconds(1));

                if (useBase64) {
                    String encKey1 = codec.encodeKey(key1);
                    String encKey2 = codec.encodeKey(key2);
                    assertEquals("$KV." + bucketName + "." + encKey1, m1.getSubject());
                    assertEquals("$KV." + bucketName + "." + encKey2, m2.getSubject());
                    Base64 base64 = new Base64();
                    assertArrayEquals(base64.encode(v1.serialize()), m1.getData());
                    assertArrayEquals(base64.encode(v2.serialize()), m2.getData());
                }
                else {
                    assertEquals("$KV." + bucketName + ".key.1", m1.getSubject());
                    assertEquals("$KV." + bucketName + ".key.2", m2.getSubject());
                    assertArrayEquals(v1.serialize(), m1.getData());
                    assertArrayEquals(v2.serialize(), m2.getData());
                }
            }
        }
    }

    @Test
    public void testKeyWorkflow() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                Codec<KeyOrValue, KeyOrValue> codec = new KeyOrValueCodec();

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                EncodedKeyValue<KeyOrValue, KeyOrValue> ekv = new EncodedKeyValue<>(nc, bucketName, codec);

                KeyOrValue key1 = new KeyOrValue("foo1", null, true);
                KeyOrValue key2 = new KeyOrValue("foo2", null, true);
                KeyOrValue v1 = new KeyOrValue("bar1", "baz1", false);
                KeyOrValue v2 = new KeyOrValue("bar2", "baz2", false);

                validatePutRevision(1, ekv.put(key1, v1));
                validatePutRevision(2, ekv.put(key2, v2));

                validateGet(key1, v1, ekv.get(key1));
                validateGet(key2, v2, ekv.get(key2));

                assertNull(ekv.get(new KeyOrValue("not-found", null, true)));

                validateKeys(key1, key2, ekv.keys());
                validateKeys(key1, key2, getFromQueue(ekv.consumeKeys()));

                assertThrows(UnsupportedOperationException.class, () -> ekv.keys(key1));
                assertThrows(UnsupportedOperationException.class, () -> ekv.consumeKeys(key1));

                List<KeyOrValue> keyList = new ArrayList<>();
                keyList.add(key1);
                keyList.add(key2);
                assertThrows(UnsupportedOperationException.class, () -> ekv.keys(keyList));
                assertThrows(UnsupportedOperationException.class, () -> ekv.consumeKeys(keyList));
            }
        }
    }

    private static void validatePutRevision(long expectedRev, long actualRev) {
        assertEquals(expectedRev, actualRev);
    }

    private static <T> void validateGet(T key, KeyOrValue value, EncodedKeyValueEntry<T, KeyOrValue> entry) throws Exception {
        assertNotNull(entry);
        assertEquals(key, entry.getKey());
        assertEquals(value, entry.getValue());
    }

    private static <T> void validateKeys(T key1, T key2, List<T> keys) {
        int count = 0;
        if (key1 != null) {
            count++;
            if (!keys.contains(key1)) {
                int x = 0;
            }
            assertTrue(keys.contains(key1));
        }
        else {
            assertFalse(keys.contains(key1));
        }
        if (key2 != null) {
            count++;
            assertTrue(keys.contains(key2));
        }
        else {
            assertFalse(keys.contains(key2));
        }
        assertEquals(count, keys.size());
    }

    private static <T> List<T> getFromQueue(EncodedKeyConsumer<T, KeyOrValue> q) throws Exception {
        List<T> keys = new ArrayList<>();
        try {
            boolean notDone = true;
            do {
                EncodedKeyResult<T, KeyOrValue> r = q.poll(100, TimeUnit.SECONDS);
                if (r != null) {
                    if (r.isDone()) {
                        notDone = false;
                    }
                    else {
                        keys.add(r.getKey());
                    }
                }
            }
            while (notDone);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return keys;
    }

    static class TestKeyValueWatcher implements EncodedKeyValueWatcher<String, String> {
        public String name;
        public List<EncodedKeyValueEntry<String, String>> entries = new ArrayList<>();
        public KeyValueWatchOption[] watchOptions;
        public boolean beforeWatcher;
        public boolean metaOnly;
        public int endOfDataReceived;
        public boolean endBeforeEntries;
        public StringAndStringCodec codec;

        public TestKeyValueWatcher(String name, boolean beforeWatcher, boolean useBase64, KeyValueWatchOption... watchOptions) {
            this.name = name;
            this.beforeWatcher = beforeWatcher;
            this.watchOptions = watchOptions;
            for (KeyValueWatchOption wo : watchOptions) {
                if (wo == META_ONLY) {
                    metaOnly = true;
                    break;
                }
            }
            codec = new StringAndStringCodec(useBase64);
        }

        @Override
        public String toString() {
            return "TestKeyValueWatcher{" +
                "name='" + name + '\'' +
                ", beforeWatcher=" + beforeWatcher +
                ", metaOnly=" + metaOnly +
                ", watchOptions=" + Arrays.toString(watchOptions) +
                '}';
        }

        @Override
        public void watch(EncodedKeyValueEntry<String, String> entry) {
            entries.add(entry);
        }

        @Override
        public void endOfData() {
            if (++endOfDataReceived == 1 && entries.isEmpty()) {
                endBeforeEntries = true;
            }
        }
    }

    static String TEST_WATCH_KEY_NULL = "key.nl";
    static String TEST_WATCH_KEY_1 = "key.1";
    static String TEST_WATCH_KEY_2 = "key.2";

    interface TestWatchSubSupplier {
        NatsKeyValueWatchSubscription get(EncodedKeyValue<String, String> kv) throws Exception;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWatch(boolean useBase64) throws Exception {
        Object[] key1AllExpecteds = new Object[]{
            "a", "aa", KeyValueOperation.DELETE, "aaa", KeyValueOperation.DELETE, KeyValueOperation.PURGE
        };

        Object[] key1FromRevisionExpecteds = new Object[]{
            "aa", KeyValueOperation.DELETE, "aaa"
        };

        Object[] noExpecteds = new Object[0];
        Object[] purgeOnlyExpecteds = new Object[]{KeyValueOperation.PURGE};

        Object[] key2AllExpecteds = new Object[]{
            "z", "zz", KeyValueOperation.DELETE, "zzz"
        };

        Object[] key2AfterExpecteds = new Object[]{"zzz"};

        Object[] allExpecteds = new Object[]{
            "a", "aa", "z", "zz",
            KeyValueOperation.DELETE, KeyValueOperation.DELETE,
            "aaa", "zzz",
            KeyValueOperation.DELETE, KeyValueOperation.PURGE,
            null
        };

        Object[] allPutsExpecteds = new Object[]{
            "a", "aa", "z", "zz", "aaa", "zzz", null
        };

        Object[] allFromRevisionExpecteds = new Object[]{
            "aa", "z", "zz",
            KeyValueOperation.DELETE, KeyValueOperation.DELETE,
            "aaa", "zzz",
        };

        TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher("key1FullWatcher", true, useBase64);
        TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher("key1MetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher key1StartNewWatcher = new TestKeyValueWatcher("key1StartNewWatcher", true, useBase64, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1StartAllWatcher = new TestKeyValueWatcher("key1StartAllWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher("key2FullWatcher", true, useBase64);
        TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher("key2MetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher allAllFullWatcher = new TestKeyValueWatcher("allAllFullWatcher", true, useBase64);
        TestKeyValueWatcher allAllMetaWatcher = new TestKeyValueWatcher("allAllMetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher allIgDelFullWatcher = new TestKeyValueWatcher("allIgDelFullWatcher", true, useBase64, IGNORE_DELETE);
        TestKeyValueWatcher allIgDelMetaWatcher = new TestKeyValueWatcher("allIgDelMetaWatcher", true, useBase64, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher("starFullWatcher", true, useBase64);
        TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher("starMetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher("gtFullWatcher", true, useBase64);
        TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher("gtMetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher multipleFullWatcher = new TestKeyValueWatcher("multipleFullWatcher", true, useBase64);
        TestKeyValueWatcher multipleMetaWatcher = new TestKeyValueWatcher("multipleMetaWatcher", true, useBase64, META_ONLY);
        TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher("key1AfterWatcher", false, useBase64, META_ONLY);
        TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher("key1AfterIgDelWatcher", false, useBase64, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher("key1AfterStartNewWatcher", false, useBase64, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher("key1AfterStartFirstWatcher", false, useBase64, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher("key2AfterWatcher", false, useBase64, META_ONLY);
        TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher("key2AfterStartNewWatcher", false, useBase64, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher("key2AfterStartFirstWatcher", false, useBase64, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key1FromRevisionAfterWatcher = new TestKeyValueWatcher("key1FromRevisionAfterWatcher", false, useBase64);
        TestKeyValueWatcher allFromRevisionAfterWatcher = new TestKeyValueWatcher("allFromRevisionAfterWatcher", false, useBase64);
        TestKeyValueWatcher key1Key2FromRevisionAfterWatcher = new TestKeyValueWatcher("key1Key2FromRevisionAfterWatcher", false, useBase64);

        List<String> allKeys = Arrays.asList(TEST_WATCH_KEY_1, TEST_WATCH_KEY_2, TEST_WATCH_KEY_NULL);

        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                _testWatch(nc, key1FullWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1FullWatcher, key1FullWatcher.watchOptions));
                _testWatch(nc, key1MetaWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1MetaWatcher, key1MetaWatcher.watchOptions));
                _testWatch(nc, key1StartNewWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartNewWatcher, key1StartNewWatcher.watchOptions));
                _testWatch(nc, key1StartAllWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartAllWatcher, key1StartAllWatcher.watchOptions));
                _testWatch(nc, key2FullWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2FullWatcher, key2FullWatcher.watchOptions));
                _testWatch(nc, key2MetaWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2MetaWatcher, key2MetaWatcher.watchOptions));
                _testWatch(nc, allAllFullWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllFullWatcher, allAllFullWatcher.watchOptions));
                _testWatch(nc, allAllMetaWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllMetaWatcher, allAllMetaWatcher.watchOptions));
                _testWatch(nc, allIgDelFullWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelFullWatcher, allIgDelFullWatcher.watchOptions));
                _testWatch(nc, allIgDelMetaWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelMetaWatcher, allIgDelMetaWatcher.watchOptions));
                _testWatch(nc, starFullWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starFullWatcher, starFullWatcher.watchOptions));
                _testWatch(nc, starMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starMetaWatcher, starMetaWatcher.watchOptions));
                _testWatch(nc, gtFullWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtFullWatcher, gtFullWatcher.watchOptions));
                _testWatch(nc, gtMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtMetaWatcher, gtMetaWatcher.watchOptions));
                _testWatch(nc, key1AfterWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterWatcher, key1AfterWatcher.watchOptions));
                _testWatch(nc, key1AfterIgDelWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.watchOptions));
                _testWatch(nc, key1AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.watchOptions));
                _testWatch(nc, key1AfterStartFirstWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.watchOptions));
                _testWatch(nc, key2AfterWatcher, key2AfterExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterWatcher, key2AfterWatcher.watchOptions));
                _testWatch(nc, key2AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.watchOptions));
                _testWatch(nc, key2AfterStartFirstWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.watchOptions));
                _testWatch(nc, key1FromRevisionAfterWatcher, key1FromRevisionExpecteds, 2, kv -> kv.watch(TEST_WATCH_KEY_1, key1FromRevisionAfterWatcher, 2, key1FromRevisionAfterWatcher.watchOptions));
                _testWatch(nc, allFromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watchAll(allFromRevisionAfterWatcher, 2, allFromRevisionAfterWatcher.watchOptions));

                List<String> keys = Arrays.asList(TEST_WATCH_KEY_1, TEST_WATCH_KEY_2);
                _testWatch(nc, key1Key2FromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watch(keys, key1Key2FromRevisionAfterWatcher, 2, key1Key2FromRevisionAfterWatcher.watchOptions));
                _testWatch(nc, multipleFullWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleFullWatcher, multipleFullWatcher.watchOptions));
                _testWatch(nc, multipleMetaWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleMetaWatcher, multipleMetaWatcher.watchOptions));
            }
        }
    }

    private void _testWatch(Connection nc, TestKeyValueWatcher watcher, Object[] expectedKves, long fromRevision, TestWatchSubSupplier supplier) throws Exception {
        KeyValueManagement kvm = nc.keyValueManagement();

        String bucket = NUID.nextGlobalSequence() + watcher.name + "Bucket";
        kvm.create(KeyValueConfiguration.builder()
            .name(bucket)
            .maxHistoryPerKey(10)
            .storageType(StorageType.Memory)
            .build());

        EncodedKeyValue<String, String> kv = new EncodedKeyValue<>(nc.keyValue(bucket), watcher.codec);

        NatsKeyValueWatchSubscription sub = null;

        if (watcher.beforeWatcher) {
            sub = supplier.get(kv);
        }

        if (fromRevision == -1) {
            kv.put(TEST_WATCH_KEY_1, "a");
            kv.put(TEST_WATCH_KEY_1, "aa");
            kv.put(TEST_WATCH_KEY_2, "z");
            kv.put(TEST_WATCH_KEY_2, "zz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.delete(TEST_WATCH_KEY_2);
            kv.put(TEST_WATCH_KEY_1, "aaa");
            kv.put(TEST_WATCH_KEY_2, "zzz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.purge(TEST_WATCH_KEY_1);
            kv.put(TEST_WATCH_KEY_NULL, null);
        }
        else {
            kv.put(TEST_WATCH_KEY_1, "a");
            kv.put(TEST_WATCH_KEY_1, "aa");
            kv.put(TEST_WATCH_KEY_2, "z");
            kv.put(TEST_WATCH_KEY_2, "zz");
            kv.delete(TEST_WATCH_KEY_1);
            kv.delete(TEST_WATCH_KEY_2);
            kv.put(TEST_WATCH_KEY_1, "aaa");
            kv.put(TEST_WATCH_KEY_2, "zzz");
        }

        if (!watcher.beforeWatcher) {
            sub = supplier.get(kv);
        }

        sleep(1500); // give time for the watches to get messages

        validateWatcher(expectedKves, watcher);
        //noinspection ConstantConditions
        sub.unsubscribe();
        kvm.delete(bucket);
    }

    private void validateWatcher(Object[] expectedKves, TestKeyValueWatcher watcher) {
        assertEquals(expectedKves.length, watcher.entries.size());
        assertEquals(1, watcher.endOfDataReceived);

        if (expectedKves.length > 0) {
            assertEquals(watcher.beforeWatcher, watcher.endBeforeEntries);
        }

        int aix = 0;
        ZonedDateTime lastCreated = ZonedDateTime.of(2000, 4, 1, 0, 0, 0, 0, ZoneId.systemDefault());
        long lastRevision = -1;

        for (EncodedKeyValueEntry<String, String> kve : watcher.entries) {
            assertTrue(kve.getCreated().isAfter(lastCreated) || kve.getCreated().isEqual(lastCreated));
            lastCreated = kve.getCreated();

            assertTrue(lastRevision < kve.getRevision());
            lastRevision = kve.getRevision();

            Object expected = expectedKves[aix++];
            if (expected == null) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                assertNull(kve.getValue());
                assertEquals(0, kve.getDataLen());
            }
            else if (expected instanceof String) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                String s = (String) expected;
                if (watcher.metaOnly) {
                    assertNull(kve.getValue());
                    assertEquals(s.length(), kve.getDataLen());
                }
                else {
                    assertNotNull(kve.getValue());
                    assertEquals(s.length(), kve.getDataLen());
                    assertEquals(s, kve.getValue());
                }
            }
            else {
                assertTrue(kve.getValue() == null || kve.getValue().isEmpty());
                assertEquals(0, kve.getDataLen());
                assertSame(expected, kve.getOperation());
            }
        }
    }
}
