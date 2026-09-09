package io.synadia.counters;

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.Source;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.SubjectTransform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.synadia.counters.CountersUtils.extractSources;
import static io.synadia.counters.CountersUtils.extractVal;
import static org.junit.jupiter.api.Assertions.*;

public class CountersTests {
    static NatsServerRunner runner;
    static Connection nc;
    static JetStreamManagement jsm;
    static JetStream js;

    @BeforeAll
    public static void beforeAll() throws Exception {
        NatsRunnerUtils.setDefaultOutputLevel(Level.WARNING);
        runner = new NatsServerRunner(false, true);
        Options options = Options.builder()
            .server(runner.getNatsLocalhostUri())
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

    private static Counters createCountersStream(String streamName, String... subjects) throws JetStreamApiException, IOException {
         return Counters.createCountersStream(nc,
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
            Counters counters = createCountersStream(streamName, wild);
            assertThrows(IllegalArgumentException.class, () -> counters.add(wild, 1));
            assertThrows(IllegalArgumentException.class, () -> counters.get(wild));

            String streamNameX = NUID.nextGlobalSequence();
            String subjectPrefixX = NUID.nextGlobalSequence();
            String wildX = subjectPrefixX + ".*";
            jsm.addStream(StreamConfiguration.builder()
                .name(streamNameX)
                .subjects(wildX)
                .storageType(StorageType.Memory)
                .build());
            assertThrows(IllegalArgumentException.class, () -> new Counters(streamNameX, nc));
        }
        catch (JetStreamApiException | IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCounterBasics() throws Exception {
        String streamName = NUID.nextGlobalSequence();
        String subjectPrefix = NUID.nextGlobalSequence();
        Counters counters = createCountersStream(streamName, subjectPrefix + ".*");

        String subject1 = subjectPrefix + "." + NUID.nextGlobalSequence();
        String subject2 = subjectPrefix + "." + NUID.nextGlobalSequence();
        String subject3 = subjectPrefix + "." + NUID.nextGlobalSequence();

        assertEquals(1, counters.add(subject1, 1).intValue());
        assertEquals(1, counters.get(subject1).intValue());
        assertEquals(3, counters.add(subject1, 2).intValue());
        assertEquals(3, counters.get(subject1).intValue());
        assertEquals(6, counters.add(subject1, 3).intValue());
        assertEquals(6, counters.get(subject1).intValue());
        assertEquals(5, counters.add(subject1, -1).intValue());
        assertEquals(5, counters.get(subject1).intValue());
        assertEquals(6, counters.increment(subject1).intValue());
        assertEquals(5, counters.decrement(subject1).intValue());

        assertEquals(5, counters.getOrElse(subject1, 99).intValue());
        assertEquals(Integer.MAX_VALUE, counters.getOrElse("not-exist", Integer.MAX_VALUE).intValue());
        assertEquals(Long.MAX_VALUE, counters.getOrElse("not-exist", Long.MAX_VALUE).longValue());

        assertEquals(-1, counters.add(subject2, -1).intValue());
        assertEquals(-1, counters.get(subject2).intValue());
        assertEquals(-3, counters.add(subject2, -2).intValue());
        assertEquals(-3, counters.get(subject2).intValue());
        assertEquals(-6, counters.add(subject2, -3).intValue());
        assertEquals(-6, counters.get(subject2).intValue());
        assertEquals(-5, counters.add(subject2, 1).intValue());
        assertEquals(-5, counters.get(subject2).intValue());

        assertEquals(Integer.MAX_VALUE, counters.setViaAdd(subject3, Integer.MAX_VALUE).intValue());
        assertEquals(Integer.MAX_VALUE, counters.get(subject3).intValue());
        assertEquals(Long.MAX_VALUE, counters.setViaAdd(subject3, Long.MAX_VALUE).longValue());
        assertEquals(Long.MAX_VALUE, counters.get(subject3).longValue());

        assertEquals(10, counters.setViaAdd(subject1, 10).intValue());
        assertEquals(100, counters.setViaAdd(subject2, 100).intValue());
        assertEquals(1000, counters.setViaAdd(subject3, 1000).intValue());
        assertEquals(10, counters.get(subject1).intValue());
        assertEquals(100, counters.get(subject2).intValue());
        assertEquals(1000, counters.get(subject3).intValue());

        BigInteger total = BigInteger.ZERO;
        LinkedBlockingQueue<CounterEntryResponse> eResponses = counters.getEntries(subject1, subject2, subject3);
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
        eResponses = counters.getEntries(subjectPrefix + ".*");
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
    public void testSourcesHeaderMatchesServer() {
        // The server emits "Nats-Counter-Sources" (nats-server server/stream.go)
        assertEquals("Nats-Counter-Sources", CountersUtils.SOURCES_HEADER);
    }

    @Test
    public void testCounterSourcesPopulatedFromSourcedStream() throws Exception {
        String prefix = "cshdr";
        Counters es = createCountersStream(prefix + "-ES", prefix + ".es.*");

        Counters eu = Counters.createCountersStream(nc,
            StreamConfiguration.builder()
                .name(prefix + "-EU")
                .subjects(prefix + ".eu.*")
                .storageType(StorageType.Memory)
                .sources(Source.builder()
                    .sourceName(prefix + "-ES")
                    .subjectTransforms(SubjectTransform.builder()
                        .source(prefix + ".es.>")
                        .destination(prefix + ".eu.>")
                        .build())
                    .build())
                .build());

        es.add(prefix + ".es.hits", 100);

        CounterEntry entry = null;
        for (int i = 0; i < 100; i++) {
            try {
                entry = eu.getEntry(prefix + ".eu.hits");
                if (!entry.getSources().isEmpty()) {
                    break;
                }
            }
            catch (Exception ignore) {
                // sourcing not propagated yet
            }
            Thread.sleep(100);
        }

        assertNotNull(entry);
        assertEquals(100, entry.getValue().intValue());
        Map<String, Map<String, BigInteger>> sources = entry.getSources();
        assertFalse(sources.isEmpty(), "sources should be populated from the sourced counter stream");
        Map<String, BigInteger> esSources = sources.get(prefix + "-ES");
        assertNotNull(esSources);
        assertEquals(100, esSources.get(prefix + ".es.hits").intValue());
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
