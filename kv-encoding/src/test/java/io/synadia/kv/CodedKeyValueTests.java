// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.kv;

import io.nats.client.Connection;
import io.nats.client.KeyValueManagement;
import io.nats.client.NUID;
import io.nats.client.Nats;
import io.nats.client.api.KeyValueConfiguration;
import io.synadia.kv.codec.Codec;
import io.synadia.kv.codec.KeyOrValue;
import io.synadia.kv.codec.KeyOrValueCodec;
import io.synadia.kv.codec.TestStringKeyCodec;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

public class CodedKeyValueTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @Test
    public void testStringKeyWorkflow() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                TestStringKeyCodec codec = new TestStringKeyCodec();

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                CodedKeyValue<String, KeyOrValue> ekv
                    = new CodedKeyValue<>(nc, bucketName, codec);

                String key1 = "key.1";
                KeyOrValue v1 = new KeyOrValue("v1", "foo", false);
                long rev1 = ekv.put(key1, v1);
                assertEquals(1, rev1);

                String key2 = "key.2";
                KeyOrValue v2 = new KeyOrValue("v2", "bar", false);
                long rev2 = ekv.put(key2, v2);
                assertEquals(2, rev2);

                CodedKeyValueEntry<String, KeyOrValue> entry = ekv.get(key1);
                assertNotNull(entry);
                assertEquals(key1, entry.getKey());
                assertEquals(v1, entry.getValue());

                entry = ekv.get(key2);
                assertNotNull(entry);
                assertEquals(key2, entry.getKey());
                assertEquals(v2, entry.getValue());

                entry = ekv.get("not-found");
                assertNull(entry);

                LinkedBlockingQueue<CodedKeyResult<String, KeyOrValue>> q = ekv.consumeKeys();
                List<String> keys = getFromStringQueue(q);
                assertEquals(2, keys.size());
                assertTrue(keys.contains(key1));
                assertTrue(keys.contains(key2));

                q = ekv.consumeKeys("key.*");
                keys = getFromStringQueue(q);
                assertEquals(2, keys.size());
                assertTrue(keys.contains(key1));
                assertTrue(keys.contains(key2));

                q = ekv.consumeKeys(key1);
                keys = getFromStringQueue(q);
                assertEquals(1, keys.size());
                assertTrue(keys.contains(key1));
                assertFalse(keys.contains(key2));
            }
        }
    }

    @Test
    public void testGeneralWorkflow() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                Codec<KeyOrValue, KeyOrValue> codec = new KeyOrValueCodec();

                String bucketName = NUID.nextGlobalSequence();
                KeyValueManagement kvm = nc.keyValueManagement();
                kvm.create(KeyValueConfiguration.builder().name(bucketName).build());

                CodedKeyValue<KeyOrValue, KeyOrValue> ekv
                    = new CodedKeyValue<>(nc, bucketName, codec);

                KeyOrValue key1 = new KeyOrValue("foo1", null, true);
                KeyOrValue v1 = new KeyOrValue("bar1", "baz1", false);
                long rev1 = ekv.put(key1, v1);
                assertEquals(1, rev1);

                KeyOrValue key2 = new KeyOrValue("foo2", null, true);
                KeyOrValue v2 = new KeyOrValue("bar2", "baz2", false);
                long rev2 = ekv.put(key2, v2);
                assertEquals(2, rev2);

                CodedKeyValueEntry<KeyOrValue, KeyOrValue> entry = ekv.get(key1);
                assertNotNull(entry);
                assertEquals(key1, entry.getKey());
                assertEquals(v1, entry.getValue());

                entry = ekv.get(key2);
                assertNotNull(entry);
                assertEquals(key2, entry.getKey());
                assertEquals(v2, entry.getValue());

                KeyOrValue key404 = new KeyOrValue("not-found", null, true);
                entry = ekv.get(key404);
                assertNull(entry);

                LinkedBlockingQueue<CodedKeyResult<KeyOrValue, KeyOrValue>> q = ekv.consumeKeys();
                List<KeyOrValue> keys = KeyOrValue(q);
                assertEquals(2, keys.size());
                assertTrue(keys.contains(key1));
                assertTrue(keys.contains(key2));

                List<KeyOrValue> keyList = new ArrayList<>();
                keyList.add(key1);
                keyList.add(key2);
                assertThrows(UnsupportedOperationException.class, () -> ekv.consumeKeys(key1));
                assertThrows(UnsupportedOperationException.class, () -> ekv.consumeKeys(keyList));
            }
        }
    }

    private static List<String> getFromStringQueue(
        LinkedBlockingQueue<CodedKeyResult<String, KeyOrValue>> q) throws Exception {
        List<String> keys = new ArrayList<>();
        try {
            boolean notDone = true;
            do {
                CodedKeyResult<String, KeyOrValue> r = q.poll(100, TimeUnit.SECONDS);
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

    private static List<KeyOrValue> KeyOrValue(
        LinkedBlockingQueue<CodedKeyResult<KeyOrValue, KeyOrValue>> q) throws Exception {
        List<KeyOrValue> keys = new ArrayList<>();
        try {
            boolean notDone = true;
            do {
                CodedKeyResult<KeyOrValue, KeyOrValue> r = q.poll(100, TimeUnit.SECONDS);
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
}
