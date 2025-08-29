package io.synadia.counter;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.synadia.counter.CounterUtils.extractSources;
import static io.synadia.counter.CounterUtils.extractVal;
import static org.junit.jupiter.api.Assertions.*;

public class CounterTests {
    static NatsServerRunner runner;
    static Connection nc;
    static JetStreamManagement jsm;
    static JetStream js;

    @BeforeAll
    public static void beforeAll() throws Exception {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        runner = new NatsServerRunner(false, true);
        Options options = Options.builder()
            .server(runner.getURI())
            .errorListener(new ErrorListener() {})
            .build();
        nc = Nats.connect(options);
        jsm = nc.jetStreamManagement();
        js = nc.jetStream();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        runner.close();
    }

    private static Counter createCounterStream(String streamName, String... subjects) throws JetStreamApiException, IOException {
         return Counter.createCounterStream(nc,
            StreamConfiguration.builder()
                .name(streamName)
                .subjects(subjects)
                .storageType(StorageType.Memory)
                .build());
    }

    @Test
    public void testCounterExceptions() {
        String streamName = NUID.nextGlobalSequence();
        String subjectPrefix = NUID.nextGlobalSequence();
        String wild = subjectPrefix + ".*";
        try {
            Counter counter = createCounterStream(streamName, wild);
            assertThrows(IllegalArgumentException.class, () -> counter.add(wild, 1));
            assertThrows(IllegalArgumentException.class, () -> counter.get(wild));

            String streamNameX = NUID.nextGlobalSequence();
            String subjectPrefixX = NUID.nextGlobalSequence();
            String wildX = subjectPrefixX + ".*";
            jsm.addStream(StreamConfiguration.builder()
                .name(streamNameX)
                .subjects(wildX)
                .storageType(StorageType.Memory)
                .build());
            assertThrows(IllegalArgumentException.class, () -> new Counter(streamNameX, nc));
        }
        catch (JetStreamApiException | IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCounterBasics() throws Exception {
        String streamName = NUID.nextGlobalSequence();
        String subjectPrefix = NUID.nextGlobalSequence();
        Counter counter = createCounterStream(streamName, subjectPrefix + ".*");

        String subject1 = subjectPrefix + "." + NUID.nextGlobalSequence();
        String subject2 = subjectPrefix + "." + NUID.nextGlobalSequence();
        String subject3 = subjectPrefix + "." + NUID.nextGlobalSequence();

        assertEquals(1, counter.add(subject1, 1).intValue());
        assertEquals(1, counter.get(subject1).intValue());
        assertEquals(3, counter.add(subject1, 2).intValue());
        assertEquals(3, counter.get(subject1).intValue());
        assertEquals(6, counter.add(subject1, 3).intValue());
        assertEquals(6, counter.get(subject1).intValue());
        assertEquals(5, counter.add(subject1, -1).intValue());
        assertEquals(5, counter.get(subject1).intValue());
        assertEquals(6, counter.increment(subject1).intValue());
        assertEquals(5, counter.decrement(subject1).intValue());

        assertEquals(5, counter.getOrElse(subject1, 99).intValue());
        assertEquals(Integer.MAX_VALUE, counter.getOrElse("not-exist", Integer.MAX_VALUE).intValue());
        assertEquals(Long.MAX_VALUE, counter.getOrElse("not-exist", Long.MAX_VALUE).longValue());

        assertEquals(-1, counter.add(subject2, -1).intValue());
        assertEquals(-1, counter.get(subject2).intValue());
        assertEquals(-3, counter.add(subject2, -2).intValue());
        assertEquals(-3, counter.get(subject2).intValue());
        assertEquals(-6, counter.add(subject2, -3).intValue());
        assertEquals(-6, counter.get(subject2).intValue());
        assertEquals(-5, counter.add(subject2, 1).intValue());
        assertEquals(-5, counter.get(subject2).intValue());

        assertEquals(Integer.MAX_VALUE, counter.setViaAdd(subject3, Integer.MAX_VALUE).intValue());
        assertEquals(Integer.MAX_VALUE, counter.get(subject3).intValue());
        assertEquals(Long.MAX_VALUE, counter.setViaAdd(subject3, Long.MAX_VALUE).longValue());
        assertEquals(Long.MAX_VALUE, counter.get(subject3).longValue());

        assertEquals(10, counter.setViaAdd(subject1, 10).intValue());
        assertEquals(100, counter.setViaAdd(subject2, 100).intValue());
        assertEquals(1000, counter.setViaAdd(subject3, 1000).intValue());
        assertEquals(10, counter.get(subject1).intValue());
        assertEquals(100, counter.get(subject2).intValue());
        assertEquals(1000, counter.get(subject3).intValue());

        BigInteger total = BigInteger.ZERO;
        LinkedBlockingQueue<CounterValueResponse> vResponses = counter.getMultiple(subject1, subject2, subject3);
        CounterValueResponse vr = vResponses.poll(1, TimeUnit.SECONDS);
        while (vr != null && vr.isValue()) {
            total = total.add(vr.getValue());
            vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
        }
        assertNotNull(vr);
        assertTrue(vr.isEobStatus());
        assertEquals(1110, total.intValue());

        total = BigInteger.ZERO;
        LinkedBlockingQueue<CounterEntryResponse> eResponses = counter.getEntries(subject1, subject2, subject3);
        CounterEntryResponse er = eResponses.poll(1, TimeUnit.SECONDS);
        while (er != null && er.isEntry()) {
            CounterEntry entry = er.getEntry();
            assertNotNull(entry);
            total = total.add(entry.getValue());
            er = eResponses.poll(10, TimeUnit.MILLISECONDS);
        }
        assertNotNull(er);
        assertTrue(er.isEobStatus());
        assertEquals(1110, total.intValue());

        total = BigInteger.ZERO;
        vResponses = counter.getMultiple(subjectPrefix + ".*");
        vr = vResponses.poll(1, TimeUnit.SECONDS);
        while (vr != null && vr.isValue()) {
            total = total.add(vr.getValue());
            vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
        }
        assertNotNull(vr);
        assertTrue(vr.isEobStatus());
        assertEquals(1110, total.intValue());

        total = BigInteger.ZERO;
        eResponses = counter.getEntries(subjectPrefix + ".*");
        er = eResponses.poll(1, TimeUnit.SECONDS);
        while (er != null && er.isEntry()) {
            CounterEntry entry = er.getEntry();
            assertNotNull(entry);
            total = total.add(entry.getValue());
            er = eResponses.poll(10, TimeUnit.MILLISECONDS);
        }
        assertNotNull(er);
        assertTrue(er.isEobStatus());
        assertEquals(1110, total.intValue());
    }

    @Test
    public void testExtractVal() {
        assertEquals(10, extractVal("{\"val\":\"10\"}".getBytes()).intValue());
        assertEquals(0, extractVal("{\"val\":\"0\"}".getBytes()).intValue());
        assertEquals(-10, extractVal("{\"val\":\"-10\"}".getBytes()).intValue());
        String l = "" + Long.MAX_VALUE;
        assertEquals(Long.MAX_VALUE, extractVal(("{\"val\":\"" + l + "\"}").getBytes()).longValue());
    }

    @Test
    public void testExtractSources() {
        Map<String, Map<String, BigInteger>> map = extractSources(null);
        assertNotNull(map);
        assertTrue(map.isEmpty());

        map = extractSources(SOURCES_JSON);
        assertNotNull(map);
        assertFalse(map.isEmpty());
        assertEquals(2, map.size());

        Map<String, BigInteger> map2 = map.get("source1");
        assertNotNull(map2);
        assertFalse(map2.isEmpty());
        assertEquals(1, map2.size());

        BigInteger value = map2.get("subject1");
        assertNotNull(value);
        assertInstanceOf(BigInteger.class, value);
        assertEquals(10, value.longValue());
    }

    private final static String SOURCES_JSON = "{\"source1\":{\"subject1\":\"10\"},\"source2\":{\"subject2\":\"20\",\"subject3\":\"90\"}}";
}
