// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The idle ping, against a server whose batch idle timeout is shortened to
 * {@value #SERVER_BATCH_TIMEOUT_SECONDS} seconds so that a lost batch shows up in seconds rather
 * than the default 10. The client still believes the timeout is 10, since
 * {@link BatchUtils#getBatchIdleTimeoutSeconds(Connection)} cannot ask, so these tests set the
 * idle ping to 1 second explicitly.
 */
public class IdlePingTests {
    static final int SERVER_BATCH_TIMEOUT_SECONDS = 3;
    static final long IDLE_MILLIS = 5000; // longer than the server timeout

    static NatsServerRunner runner;
    static Connection nc;
    static JetStreamManagement jsm;

    @BeforeAll
    public static void beforeAll() throws Exception {
        NatsRunnerUtils.setDefaultOutputLevel(Level.WARNING);
        runner = NatsServerRunner.builder()
            .jetstream(true)
            .configInserts(new String[]{
                "jetstream { limits { batch { timeout: " + SERVER_BATCH_TIMEOUT_SECONDS + "s } } }"})
            .build();
        Options options = Options.builder()
            .server(runner.getNatsLocalhostUri())
            .errorListener(new ErrorListener() {})
            .build();
        nc = Nats.connect(options);
        jsm = nc.jetStreamManagement();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (nc != null) {
            nc.close();
        }
        if (runner != null) {
            runner.close();
        }
    }

    // ----------------------------------------------------------------------------------
    // helpers
    // ----------------------------------------------------------------------------------
    private static byte[] data(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String createStream(String subject) throws Exception {
        String streamName = NUID.nextGlobalSequence();
        jsm.addStream(StreamConfiguration.builder()
            .name(streamName)
            .subjects(subject)
            .storageType(StorageType.Memory)
            .allowBatched(true)
            .build());
        return streamName;
    }

    private static FastPublisher.Builder builder() {
        return FastPublisher.builder().connection(nc).ackTimeout(10_000);
    }

    /**
     * Watch the subject for pings, keeping each one's batch sequence. The reply subject ends
     * {@code <seq>.<op>.$FI}, and a ping is operation 4.
     */
    private static List<Long> watchPings(String subject) {
        List<Long> pingSeqs = new CopyOnWriteArrayList<>();
        Dispatcher d = nc.createDispatcher(m -> {
            String reply = m.getReplyTo();
            if (reply != null) {
                String[] tokens = reply.split("\\.");
                if ("4".equals(tokens[tokens.length - 2])) {
                    pingSeqs.add(Long.parseLong(tokens[tokens.length - 3]));
                }
            }
        });
        d.subscribe(subject);
        return pingSeqs;
    }

    private static int idlePingSecondsOf(FastPublisher.Builder b) {
        try (FastPublisher fp = b.build()) {
            return fp.getIdlePingSeconds();
        }
    }

    // ----------------------------------------------------------------------------------
    // tests
    // ----------------------------------------------------------------------------------
    @Test
    public void testIdlePingKeepsTheBatchAlive() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(subject);

        FastPublisher fp = builder().idlePingSeconds(1).build();
        fp.add(subject, data("1"));
        fp.add(subject, data("2"));
        Thread.sleep(IDLE_MILLIS);
        PublishAck pa = fp.closeBatch();

        assertEquals(2, pa.getBatchSize());
        assertEquals(2, jsm.getStreamInfo(streamName).getStreamState().getMsgCount());
    }

    @Test
    public void testWithoutIdlePingTheServerAbandonsTheBatch() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(subject);

        FastPublisher fp = builder().idlePingSeconds(0).build();
        fp.add(subject, data("1"));
        Thread.sleep(IDLE_MILLIS);
        FastPublishException e = assertThrows(FastPublishException.class, fp::closeBatch);
        assertTrue(e.getMessage().contains("10208"), e.getMessage());

        // the server refused the close, so the batch ended on an error, not a commit
        assertTrue(fp.isTerminal());
        assertEquals(EndReason.Error, fp.getEndReason());

        // and nothing more is coming, so a second close fails at once rather than waiting out
        // the ack timeout for a terminal ack that has already arrived
        long start = System.currentTimeMillis();
        FastPublishException again = assertThrows(FastPublishException.class, fp::closeBatch);
        assertNull(again.getPublishAck());
        assertTrue(System.currentTimeMillis() - start < 1000, "a second close must not wait for an ack");
    }

    @Test
    public void testIdlePingCarriesTheSentSequenceAndStopsAtCloseBatch() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(subject);
        List<Long> pingSeqs = watchPings(subject);

        FastPublisher fp = builder().idlePingSeconds(1).build();
        fp.add(subject, data("1"));
        fp.add(subject, data("2"));
        fp.add(subject, data("3"));
        Thread.sleep(2500);

        int pings = pingSeqs.size();
        assertTrue(pings >= 2, "expected at least 2 idle pings in 2.5 seconds, got " + pings);
        for (long seq : pingSeqs) {
            assertEquals(3, seq, "an idle ping must carry the highest sequence sent, never more");
        }
        assertEquals(3, fp.size(), "an idle ping must not consume a batch sequence");

        fp.closeBatch();
        Thread.sleep(2500);
        assertEquals(pings, pingSeqs.size(), "no idle ping may follow closeBatch");
    }

    @Test
    public void testNoIdlePingBeforeTheFirstMessageOrAfterClose() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(subject);
        List<Long> pingSeqs = watchPings(subject);

        FastPublisher fp = builder().idlePingSeconds(1).build();
        Thread.sleep(1500);
        assertEquals(0, pingSeqs.size(), "a batch with no messages has nothing to keep alive");

        fp.add(subject, data("1"));
        fp.close();
        Thread.sleep(1500);
        assertEquals(0, pingSeqs.size(), "no idle ping may follow close");
    }

    @Test
    public void testIdlePingSecondsBounds() {
        assertEquals(10, BatchUtils.getBatchIdleTimeoutSeconds(nc));
        assertEquals(5, BatchUtils.getDefaultIdlePingSeconds(nc));
        assertEquals(8, BatchUtils.getMaxIdlePingSeconds(nc));

        assertEquals(5, idlePingSecondsOf(builder()));
        assertEquals(1, idlePingSecondsOf(builder().idlePingSeconds(1)));
        assertEquals(8, idlePingSecondsOf(builder().idlePingSeconds(8)));
        assertEquals(0, idlePingSecondsOf(builder().idlePingSeconds(0)));
        assertEquals(0, idlePingSecondsOf(builder().idlePingSeconds(-1)));
        assertThrows(IllegalArgumentException.class, () -> builder().idlePingSeconds(9).build());
        assertThrows(IllegalArgumentException.class,
            () -> FastPublisher.builder().connection(nc).idlePingSeconds(9).build());
    }
}
