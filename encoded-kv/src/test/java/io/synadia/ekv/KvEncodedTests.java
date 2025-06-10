// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.StorageType;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.synadia.ekv.codec.KeyCodec;
import io.synadia.ekv.codec.StringKeyCodec;
import nats.io.NatsServerRunner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
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

public class KvEncodedTests {
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

        GeneralKeyCodec codec = new GeneralKeyCodec(GeneralType.BASE64);
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

        GeneralKeyCodec codec = new GeneralKeyCodec(GeneralType.HEX);
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

    @ParameterizedTest
    @EnumSource(GeneralType.class)
    public void testStringKeyWorkflow(GeneralType gt) throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                GeneralKeyCodec keyCodec = new GeneralKeyCodec(gt);
                DataValueCodec dvc = new DataValueCodec(gt);

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                // this is just for coverage of constructors.
                KvEncodedKeyEncodedValue<String, Data> ekv;
                if (gt == GeneralType.PLAIN) {
                    KeyValue kv = nc.keyValue(bucketName);
                    ekv = new KvEncodedKeyEncodedValue<>(kv, keyCodec, dvc);
                }
                else {
                    ekv = new KvEncodedKeyEncodedValue<>(nc, bucketName, keyCodec, dvc);
                }

                String key1 = "key.1";
                String key2 = "key.2";
                List<String> keyList = new ArrayList<>();
                keyList.add(key1);
                keyList.add(key2);

                Data v1 = new Data("v1", "foo", false);
                Data v2 = new Data("v2", "bar", false);

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

                switch (gt) {
                    case PLAIN:
                        assertEquals("$KV." + bucketName + ".key.1", m1.getSubject());
                        assertEquals("$KV." + bucketName + ".key.2", m2.getSubject());
                        assertArrayEquals(v1.serialize(), m1.getData());
                        assertArrayEquals(v2.serialize(), m2.getData());
                        break;
                    case BASE64:
                        String encKey1 = keyCodec.encode(key1);
                        String encKey2 = keyCodec.encode(key2);
                        assertEquals("$KV." + bucketName + "." + encKey1, m1.getSubject());
                        assertEquals("$KV." + bucketName + "." + encKey2, m2.getSubject());
                        Base64 base64 = new Base64();
                        assertArrayEquals(base64.encode(v1.serialize()), m1.getData());
                        assertArrayEquals(base64.encode(v2.serialize()), m2.getData());
                        break;
                    case HEX:
                        String encKeyH1 = keyCodec.encode(key1);
                        String encKeyH2 = keyCodec.encode(key2);
                        assertEquals("$KV." + bucketName + "." + encKeyH1, m1.getSubject());
                        assertEquals("$KV." + bucketName + "." + encKeyH2, m2.getSubject());
                        Hex hex = new Hex();
                        assertArrayEquals(hex.encode(v1.serialize()), m1.getData());
                        assertArrayEquals(hex.encode(v2.serialize()), m2.getData());
                }
            }
        }
    }

    @Test
    public void testKeyWorkflow() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                DataKeyCodec dkc = new DataKeyCodec();
                DataValueCodec dvc = new DataValueCodec(GeneralType.BASE64);

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                KvEncodedKeyEncodedValue<Data, Data> ekv = new KvEncodedKeyEncodedValue<>(nc, bucketName, dkc, dvc);

                Data key1 = new Data("foo1", null, true);
                Data key2 = new Data("foo2", null, true);
                Data v1 = new Data("bar1", "baz1", false);
                Data v2 = new Data("bar2", "baz2", false);

                validatePutRevision(1, ekv.put(key1, v1));
                validatePutRevision(2, ekv.put(key2, v2));

                validateGet(key1, v1, ekv.get(key1));
                validateGet(key2, v2, ekv.get(key2));

                assertNull(ekv.get(new Data("not-found", null, true)));

                validateKeys(key1, key2, ekv.keys());
                validateKeys(key1, key2, getFromQueue(ekv.consumeKeys()));

                assertThrows(UnsupportedOperationException.class, () -> ekv.keys(key1));
                assertThrows(UnsupportedOperationException.class, () -> ekv.consumeKeys(key1));

                List<Data> keyList = new ArrayList<>();
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

    private static <T> void validateGet(T key, Data value, EncodedKeyValueEntry<T, Data> entry) throws Exception {
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

    private static <T> List<T> getFromQueue(EncodedKeyConsumer<T> q) throws Exception {
        List<T> keys = new ArrayList<>();
        try {
            boolean notDone = true;
            do {
                EncodedKeyResult<T> r = q.poll(100, TimeUnit.SECONDS);
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
        public KeyCodec<String> keyCodec;
        public GeneralValueCodec valueCodec;

        public TestKeyValueWatcher(String name, boolean beforeWatcher, GeneralType gt, KeyValueWatchOption... watchOptions) {
            this.name = name;
            this.beforeWatcher = beforeWatcher;
            this.watchOptions = watchOptions;
            for (KeyValueWatchOption wo : watchOptions) {
                if (wo == META_ONLY) {
                    metaOnly = true;
                    break;
                }
            }
            keyCodec = new GeneralKeyCodec(gt);
            valueCodec = new GeneralValueCodec(gt);
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
        NatsKeyValueWatchSubscription get(KvEncodedKeyEncodedValue<String, String> kv) throws Exception;
    }

    @ParameterizedTest
    @EnumSource(GeneralType.class)
    public void testWatch(GeneralType gt) throws Exception {
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

        TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher("key1FullWatcher", true, gt);
        TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher("key1MetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher key1StartNewWatcher = new TestKeyValueWatcher("key1StartNewWatcher", true, gt, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1StartAllWatcher = new TestKeyValueWatcher("key1StartAllWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher("key2FullWatcher", true, gt);
        TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher("key2MetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher allAllFullWatcher = new TestKeyValueWatcher("allAllFullWatcher", true, gt);
        TestKeyValueWatcher allAllMetaWatcher = new TestKeyValueWatcher("allAllMetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher allIgDelFullWatcher = new TestKeyValueWatcher("allIgDelFullWatcher", true, gt, IGNORE_DELETE);
        TestKeyValueWatcher allIgDelMetaWatcher = new TestKeyValueWatcher("allIgDelMetaWatcher", true, gt, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher("starFullWatcher", true, gt);
        TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher("starMetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher("gtFullWatcher", true, gt);
        TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher("gtMetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher multipleFullWatcher = new TestKeyValueWatcher("multipleFullWatcher", true, gt);
        TestKeyValueWatcher multipleMetaWatcher = new TestKeyValueWatcher("multipleMetaWatcher", true, gt, META_ONLY);
        TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher("key1AfterWatcher", false, gt, META_ONLY);
        TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher("key1AfterIgDelWatcher", false, gt, META_ONLY, IGNORE_DELETE);
        TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher("key1AfterStartNewWatcher", false, gt, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher("key1AfterStartFirstWatcher", false, gt, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher("key2AfterWatcher", false, gt, META_ONLY);
        TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher("key2AfterStartNewWatcher", false, gt, META_ONLY, UPDATES_ONLY);
        TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher("key2AfterStartFirstWatcher", false, gt, META_ONLY, INCLUDE_HISTORY);
        TestKeyValueWatcher key1FromRevisionAfterWatcher = new TestKeyValueWatcher("key1FromRevisionAfterWatcher", false, gt);
        TestKeyValueWatcher allFromRevisionAfterWatcher = new TestKeyValueWatcher("allFromRevisionAfterWatcher", false, gt);
        TestKeyValueWatcher key1Key2FromRevisionAfterWatcher = new TestKeyValueWatcher("key1Key2FromRevisionAfterWatcher", false, gt);

        List<String> allKeys = Arrays.asList(TEST_WATCH_KEY_1, TEST_WATCH_KEY_2, TEST_WATCH_KEY_NULL);

        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                _testWatch(nc, gt, key1FullWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1FullWatcher, key1FullWatcher.watchOptions));
                _testWatch(nc, gt, key1MetaWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1MetaWatcher, key1MetaWatcher.watchOptions));
                _testWatch(nc, gt, key1StartNewWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartNewWatcher, key1StartNewWatcher.watchOptions));
                _testWatch(nc, gt, key1StartAllWatcher, key1AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1StartAllWatcher, key1StartAllWatcher.watchOptions));
                _testWatch(nc, gt, key2FullWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2FullWatcher, key2FullWatcher.watchOptions));
                _testWatch(nc, gt, key2MetaWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2MetaWatcher, key2MetaWatcher.watchOptions));
                _testWatch(nc, gt, allAllFullWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllFullWatcher, allAllFullWatcher.watchOptions));
                _testWatch(nc, gt, allAllMetaWatcher, allExpecteds, -1, kv -> kv.watchAll(allAllMetaWatcher, allAllMetaWatcher.watchOptions));
                _testWatch(nc, gt, allIgDelFullWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelFullWatcher, allIgDelFullWatcher.watchOptions));
                _testWatch(nc, gt, allIgDelMetaWatcher, allPutsExpecteds, -1, kv -> kv.watchAll(allIgDelMetaWatcher, allIgDelMetaWatcher.watchOptions));
                _testWatch(nc, gt, starFullWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starFullWatcher, starFullWatcher.watchOptions));
                _testWatch(nc, gt, starMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.*", starMetaWatcher, starMetaWatcher.watchOptions));
                _testWatch(nc, gt, gtFullWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtFullWatcher, gtFullWatcher.watchOptions));
                _testWatch(nc, gt, gtMetaWatcher, allExpecteds, -1, kv -> kv.watch("key.>", gtMetaWatcher, gtMetaWatcher.watchOptions));
                _testWatch(nc, gt, key1AfterWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterWatcher, key1AfterWatcher.watchOptions));
                _testWatch(nc, gt, key1AfterIgDelWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.watchOptions));
                _testWatch(nc, gt, key1AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.watchOptions));
                _testWatch(nc, gt, key1AfterStartFirstWatcher, purgeOnlyExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.watchOptions));
                _testWatch(nc, gt, key2AfterWatcher, key2AfterExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterWatcher, key2AfterWatcher.watchOptions));
                _testWatch(nc, gt, key2AfterStartNewWatcher, noExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.watchOptions));
                _testWatch(nc, gt, key2AfterStartFirstWatcher, key2AllExpecteds, -1, kv -> kv.watch(TEST_WATCH_KEY_2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.watchOptions));
                _testWatch(nc, gt, key1FromRevisionAfterWatcher, key1FromRevisionExpecteds, 2, kv -> kv.watch(TEST_WATCH_KEY_1, key1FromRevisionAfterWatcher, 2, key1FromRevisionAfterWatcher.watchOptions));
                _testWatch(nc, gt, allFromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watchAll(allFromRevisionAfterWatcher, 2, allFromRevisionAfterWatcher.watchOptions));

                List<String> keys = Arrays.asList(TEST_WATCH_KEY_1, TEST_WATCH_KEY_2);
                _testWatch(nc, gt, key1Key2FromRevisionAfterWatcher, allFromRevisionExpecteds, 2, kv -> kv.watch(keys, key1Key2FromRevisionAfterWatcher, 2, key1Key2FromRevisionAfterWatcher.watchOptions));
                _testWatch(nc, gt, multipleFullWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleFullWatcher, multipleFullWatcher.watchOptions));
                _testWatch(nc, gt, multipleMetaWatcher, allExpecteds, -1, kv -> kv.watch(allKeys, multipleMetaWatcher, multipleMetaWatcher.watchOptions));
            }
        }
    }

    private void _testWatch(Connection nc, GeneralType gt, TestKeyValueWatcher watcher, Object[] expectedKves, long fromRevision, TestWatchSubSupplier supplier) throws Exception {
        KeyValueManagement kvm = nc.keyValueManagement();

        String bucket = NUID.nextGlobalSequence() + watcher.name + "Bucket";
        kvm.create(KeyValueConfiguration.builder()
            .name(bucket)
            .maxHistoryPerKey(10)
            .storageType(StorageType.Memory)
            .build());

        KvEncodedKeyEncodedValue<String, String> kv = new KvEncodedKeyEncodedValue<>(nc.keyValue(bucket), watcher.keyCodec, watcher.valueCodec);

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

        validateWatcher(gt, expectedKves, watcher);
        //noinspection ConstantConditions
        sub.unsubscribe();
        kvm.delete(bucket);
    }

    private void validateWatcher(GeneralType gt, Object[] expectedKves, TestKeyValueWatcher watcher) {
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
                assertEquals(0, kve.getEncodedDataLen());
            }
            else if (expected instanceof String) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                String s = (String) expected;
                int encodedDataLen = s.length();
                switch (gt) {
                    case BASE64:
                        encodedDataLen = Base64.encodeBase64(s.getBytes(StandardCharsets.UTF_8)).length;
                        break;
                    case HEX:
                        encodedDataLen = Hex.encodeHex(s.getBytes(StandardCharsets.UTF_8)).length;
                        break;
                }
                assertEquals(encodedDataLen, kve.getEncodedDataLen());
                if (watcher.metaOnly) {
                    assertNull(kve.getValue());
                }
                else {
                    assertNotNull(kve.getValue());
                    assertEquals(s, kve.getValue());
                }
            }
            else {
                assertTrue(kve.getValue() == null || kve.getValue().isEmpty());
                assertEquals(0, kve.getEncodedDataLen());
                assertSame(expected, kve.getOperation());
            }
        }
    }
}
