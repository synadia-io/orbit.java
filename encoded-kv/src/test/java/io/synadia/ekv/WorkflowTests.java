// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.ekv;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatchOption;
import io.nats.client.api.StorageType;
import io.nats.client.impl.NatsKeyValueWatchSubscription;
import io.synadia.ekv.codec.*;
import io.synadia.ekv.misc.Data;
import io.synadia.ekv.misc.GeneralType;
import nats.io.NatsServerRunner;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

public class WorkflowTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @ParameterizedTest
    @CsvSource({"PLAIN,N/A", "PLAIN,EVC", "PLAIN,EVC-NC", "PLAIN,EVC-KVO", "BASE64,N/A", "HEX,N/A"})
    public void testStringKeyWorkflow(String gtName, String variant) throws Exception {
        GeneralType gt = gtName.equals("PLAIN") ? GeneralType.PLAIN : (gtName.equals("BASE64") ? GeneralType.BASE64 : GeneralType.HEX);
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                GeneralStringKeyCodec keyCodec = new GeneralStringKeyCodec(gt);
                DataValueCodec dvc = new DataValueCodec(gt);

                String bucket = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucket)
                    .maxHistoryPerKey(3)
                    .build());

                // this is just for coverage of constructors and KvEncoded classes
                KvEncoded<String, Data> ekv;
                if (gt == GeneralType.PLAIN) {
                    switch (variant) {
                        case "EVC":
                            KeyValue kv = nc.keyValue(bucket);
                            ekv = new KvEncodedValue<>(kv, dvc);
                            break;
                        case "EVC-NC":
                            ekv = new KvEncodedValue<>(nc, bucket, dvc);
                            break;
                        case "EVC-KVO":
                            ekv = new KvEncodedValue<>(nc, bucket, dvc, null); // COVERAGE
                            break;
                        default:
                            ekv = new KvEncoded<>(nc, bucket, keyCodec, dvc);
                    }
                }
                else if (gt == GeneralType.BASE64) {
                    KeyValue kv = nc.keyValue(bucket);
                    ekv = new KvEncoded<>(kv, keyCodec, dvc);
                }
                else {
                    ekv = new KvEncoded<>(nc, bucket, keyCodec, dvc);
                }

                assertEquals(bucket, ekv.getBucketName());

                String key1 = "key.1";
                String key2 = "key.2";
                List<String> keyList = new ArrayList<>();
                keyList.add(key1);
                keyList.add(key2);

                Data v1 = new Data("v1", "foo", false);
                Data v2 = new Data("v2", "bar", false);

                validateRevision(1, ekv.put(key1, v1));
                validateRevision(2, ekv.put(key2, v2));

                validateGet(bucket, key1, v1, ekv.get(key1));
                validateGet(bucket, key1, v1, ekv.get(key1, 1)); // COVERAGE for revision
                validateGet(bucket, key2, v2, ekv.get(key2));
                validateGet(bucket, key2, v2, ekv.get(key2, 2)); // COVERAGE for revision

                assertNull(ekv.get(key1, 99));
                assertNull(ekv.get(key2, 99));
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

                String stream = "KV_" + bucket;
                JetStreamSubscription sub = nc.jetStream().subscribe(">", PushSubscribeOptions.builder().stream(stream).build());
                Message m1 = sub.nextMessage(Duration.ofSeconds(1));
                Message m2 = sub.nextMessage(Duration.ofSeconds(1));

                switch (gt) {
                    case PLAIN:
                        assertEquals("$KV." + bucket + ".key.1", m1.getSubject());
                        assertEquals("$KV." + bucket + ".key.2", m2.getSubject());
                        assertArrayEquals(v1.serialize(), m1.getData());
                        assertArrayEquals(v2.serialize(), m2.getData());
                        break;
                    case BASE64:
                        String encKey1 = keyCodec.encode(key1);
                        String encKey2 = keyCodec.encode(key2);
                        assertEquals("$KV." + bucket + "." + encKey1, m1.getSubject());
                        assertEquals("$KV." + bucket + "." + encKey2, m2.getSubject());
                        Base64 base64 = new Base64();
                        assertArrayEquals(base64.encode(v1.serialize()), m1.getData());
                        assertArrayEquals(base64.encode(v2.serialize()), m2.getData());
                        break;
                    case HEX:
                        String encKeyH1 = keyCodec.encode(key1);
                        String encKeyH2 = keyCodec.encode(key2);
                        assertEquals("$KV." + bucket + "." + encKeyH1, m1.getSubject());
                        assertEquals("$KV." + bucket + "." + encKeyH2, m2.getSubject());
                        Hex hex = new Hex();
                        assertArrayEquals(hex.encode(v1.serialize()), m1.getData());
                        assertArrayEquals(hex.encode(v2.serialize()), m2.getData());
                }

                String key3 = "key.3";
                Data v3a = new Data("v3", "aaa", false);
                Data v3b = new Data("v3", "bbb", false);
                Data v3c = new Data("v3", "ccc", false);
                Data v3d = new Data("v3", "ddd", false);
                validateRevision(3, ekv.create(key3, v3a));
                validateGet(bucket, key3, v3a, ekv.get(key3));

                List<Object> dataHistory = new ArrayList<>();
                dataHistory.add(v3a);
                assertHistory(dataHistory, ekv.history(key3));

                assertThrows(JetStreamApiException.class, () -> ekv.create(key3, v3a));

                validateRevision(4, ekv.update(key3, v3b, 3));
                validateGet(bucket, key3, v3b, ekv.get(key3));
                dataHistory.add(v3b);
                assertHistory(dataHistory, ekv.history(key3));

                assertThrows(JetStreamApiException.class, () -> ekv.delete(key3, 3)); // COVERAGE
                assertThrows(JetStreamApiException.class, () -> ekv.purge(key3, 3)); // COVERAGE

                ekv.delete(key3);
                assertNull(ekv.get(key3));

                dataHistory.add(KeyValueOperation.DELETE);
                assertHistory(dataHistory, ekv.history(key3));

                // revision is 6 b/c delete
                validateRevision(6, ekv.put(key3, v3c));
                validateGet(bucket, key3, v3c, ekv.get(key3));

                dataHistory.clear();
                dataHistory.add(v3b);
                dataHistory.add(KeyValueOperation.DELETE);
                dataHistory.add(v3c);
                assertHistory(dataHistory, ekv.history(key3));

                ekv.delete(key3, 6);
                assertNull(ekv.get(key3));
                dataHistory.clear();
                dataHistory.add(KeyValueOperation.DELETE);
                dataHistory.add(v3c);
                dataHistory.add(KeyValueOperation.DELETE);
                assertHistory(dataHistory, ekv.history(key3));

                // revision is 8 b/c delete
                validateRevision(8, ekv.put(key3, v3d));
                validateGet(bucket, key3, v3d, ekv.get(key3));
                dataHistory.remove(0); // delete is replaced
                dataHistory.add(v3d);

                assertHistory(dataHistory, ekv.history(key3));
                ekv.purge(key3, 8);
                assertNull(ekv.get(key3));
                dataHistory.clear();
                dataHistory.add(KeyValueOperation.PURGE);
                assertHistory(dataHistory, ekv.history(key3));
            }
        }
    }

    private void assertHistory(List<Object> expected, List<EncodedKeyValueEntry<String, Data>> apiHistory) {
        System.out.println();
        for (int x = 0; x < apiHistory.size(); x++) {
            Object o = expected.get(x);
            if (o instanceof KeyValueOperation) {
                assertEquals(o, apiHistory.get(x).getOperation());
            }
            else {
                assertEquals(o, apiHistory.get(x).getValue());
            }
        }
        assertEquals(apiHistory.size(), expected.size());
    }

    @Test
    public void testKeyWorkflow() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                DataKeyCodec dkc = new DataKeyCodec();
                DataValueCodec dvc = new DataValueCodec(GeneralType.BASE64);

                String bucket = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucket).build());

                KvEncoded<Data, Data> ekv = new KvEncoded<>(nc, bucket, dkc, dvc);

                Data key1 = new Data("foo1", null, true);
                Data key2 = new Data("foo2", null, true);
                Data v1 = new Data("bar1", "baz1", false);
                Data v2 = new Data("bar2", "baz2", false);

                validateRevision(1, ekv.put(key1, v1));
                validateRevision(2, ekv.put(key2, v2));

                validateGet(bucket, key1, v1, ekv.get(key1));
                validateGet(bucket, key1, v1, ekv.get(key1, 1)); // COVERAGE for revision
                validateGet(bucket, key2, v2, ekv.get(key2));
                validateGet(bucket, key2, v2, ekv.get(key2, 2)); // COVERAGE for revision

                assertNull(ekv.get(key1, 99));
                assertNull(ekv.get(key2, 99));
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

    private static void validateRevision(long expectedRev, long actualRev) {
        assertEquals(expectedRev, actualRev);
    }

    private static <T> void validateGet(String bucket, T key, Data value, EncodedKeyValueEntry<T, Data> entry) {
        assertNotNull(entry);
        assertEquals(bucket, entry.getBucket());
        assertEquals(key, entry.getKey());
        assertEquals(value, entry.getValue());
        assertTrue(entry.getRevision() >= 0); // COVERAGE
        assertTrue(entry.getDelta() >= 0); // COVERAGE
    }

    private static <T> void validateKeys(T key1, T key2, List<T> keys) {
        int count = 0;
        if (key1 != null) {
            count++;
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
        public ValueCodec<String> valueCodec;

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
            keyCodec = new GeneralStringKeyCodec(gt);
            valueCodec = new GeneralStringValueCodec(gt);
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
        NatsKeyValueWatchSubscription get(KvEncoded<String, String> kv) throws Exception;
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

        KvEncoded<String, String> kv = new KvEncoded<>(nc.keyValue(bucket), watcher.keyCodec, watcher.valueCodec);

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
