// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.sm;

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.logging.Level;

import static io.nats.client.support.NatsJetStreamConstants.NATS_SCHEDULE_HDR;
import static org.junit.jupiter.api.Assertions.*;

public class ScheduleManagementTests {

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
        if (nc != null) nc.close();
        if (runner != null) runner.close();
    }

    // -- helpers ---------------------------------------------------------------

    /** Holder for a freshly created stream and the schedule / target subject prefixes scoped to it. */
    private static class Fixture {
        final String stream;
        final String schedPrefix;   // e.g. "sched.<id>"
        final String tgtPrefix;     // e.g. "tgt.<id>"
        Fixture(String stream, String schedPrefix, String tgtPrefix) {
            this.stream = stream;
            this.schedPrefix = schedPrefix;
            this.tgtPrefix = tgtPrefix;
        }
        String sched(String leaf) { return schedPrefix + "." + leaf; }
        String tgt(String leaf)   { return tgtPrefix   + "." + leaf; }
    }

    /** Create a schedulable stream with unique schedule and target subject patterns. */
    private static Fixture newFixture() throws Exception {
        String id = NUID.nextGlobalSequence();
        String stream = "stream_" + id;
        String schedPrefix = "sched_" + id;
        String tgtPrefix   = "tgt_"   + id;
        ScheduleManagement.createSchedulableStream(
            jsm, stream, StorageType.Memory,
            schedPrefix + ".>",
            tgtPrefix   + ".>");
        return new Fixture(stream, schedPrefix, tgtPrefix);
    }

    /**
     * Schedule a single delayed message far enough in the future that it cannot fire
     * during the test. Returns the stream sequence the schedule message landed at.
     */
    private static long scheduleInTheFuture(String schedSubject, String targetSubject, String data) throws Exception {
        return new ScheduledMessageBuilder()
            .scheduleSubject(schedSubject)
            .targetSubject(targetSubject)
            .scheduleIn(Duration.ofHours(1))
            .data(data)
            .scheduleMessage(js);
    }

    /** True iff a schedule message still exists on the subject (carries the Nats-Schedule header). */
    private static boolean scheduleExists(String streamName, String schedSubject) throws Exception {
        try {
            MessageInfo mi = jsm.getLastMessage(streamName, schedSubject);
            return mi != null
                && mi.getHeaders() != null
                && mi.getHeaders().containsKey(NATS_SCHEDULE_HDR);
        }
        catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == 10037) {
                return false;
            }
            throw e;
        }
    }

    // -- cancelSchedule(jsm, stream, scheduleStreamSequence) -------------------

    @Test
    public void testCancelBySequence() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("a");
        long seq = scheduleInTheFuture(schedSubject, f.tgt("a"), "body");

        assertTrue(scheduleExists(f.stream, schedSubject));
        assertEquals(ScheduleManagement.Result.SUCCESS, ScheduleManagement.cancelSchedule(jsm, f.stream, seq));
        assertFalse(scheduleExists(f.stream, schedSubject));

        assertEquals(ScheduleManagement.Result.NOT_FOUND,
            ScheduleManagement.cancelSchedule(jsm, f.stream, 99_999L));
    }

    // -- cancelSchedule(jsm, scheduleSubject) ----------------------------------

    @Test
    public void testCancelBySubject_success() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("b");
        scheduleInTheFuture(schedSubject, f.tgt("b"), "body");

        assertTrue(scheduleExists(f.stream, schedSubject));
        assertEquals(ScheduleManagement.Result.SUCCESS,
            ScheduleManagement.cancelSchedule(jsm, schedSubject));
        assertFalse(scheduleExists(f.stream, schedSubject));
    }

    @Test
    public void testCancelBySubject_noStreamThrows() {
        String orphan = "no_such_subject_" + NUID.nextGlobalSequence();
        assertThrows(IllegalStateException.class,
            () -> ScheduleManagement.cancelSchedule(jsm, orphan));
    }

    // -- cancelSchedule(jsm, scheduleSubject, scheduleStream) ------------------

    @Test
    public void testCancelBySubjectInStream_exact_subject() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("c");
        scheduleInTheFuture(schedSubject, f.tgt("c"), "body");

        assertTrue(scheduleExists(f.stream, schedSubject));
        assertEquals(ScheduleManagement.Result.SUCCESS,
            ScheduleManagement.cancelSchedule(jsm, schedSubject, f.stream));
        assertFalse(scheduleExists(f.stream, schedSubject));

        assertEquals(ScheduleManagement.Result.NOT_FOUND,
            ScheduleManagement.cancelSchedule(jsm, f.sched("nope"), f.stream));
    }

    @Test
    public void testCancelBySubjectInStream_wildcard_purgesAll() throws Exception {
        Fixture f = newFixture();
        String s1 = f.sched("w1");
        String s2 = f.sched("w2");
        String s3 = f.sched("w3");
        scheduleInTheFuture(s1, f.tgt("w1"), "1");
        scheduleInTheFuture(s2, f.tgt("w2"), "2");
        scheduleInTheFuture(s3, f.tgt("w3"), "3");

        assertEquals(ScheduleManagement.Result.SUCCESS,
            ScheduleManagement.cancelSchedule(jsm, f.schedPrefix + ".*", f.stream));
        assertFalse(scheduleExists(f.stream, s1));
        assertFalse(scheduleExists(f.stream, s2));
        assertFalse(scheduleExists(f.stream, s3));
    }

    @Test
    public void testCancelBySubjectInStream_wildcard_noMatches() throws Exception {
        Fixture f = newFixture();
        assertEquals(ScheduleManagement.Result.NOT_FOUND,
            ScheduleManagement.cancelSchedule(jsm, f.schedPrefix + ".*", f.stream));
    }

    // -- publishAndCancelSchedule(jsm, sched, tgt, data, publishOnlyIfExists) --

    @Test
    public void testPublishAndCancel_unconditional_success() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("p1");
        String tgtSubject   = f.tgt("p1");
        scheduleInTheFuture(schedSubject, tgtSubject, "body");

        PublishAck ack = ScheduleManagement.publishAndCancelSchedule(
            jsm, schedSubject, tgtSubject, "cancel-now".getBytes(), null, false);

        assertNotNull(ack);
        assertFalse(scheduleExists(f.stream, schedSubject));
    }

    @Test
    public void testPublishAndCancel_ifExists_whenPresent() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("p2");
        String tgtSubject   = f.tgt("p2");
        scheduleInTheFuture(schedSubject, tgtSubject, "body");

        PublishAck ack = ScheduleManagement.publishAndCancelSchedule(
            jsm, schedSubject, tgtSubject, "cancel-now".getBytes(), null, true);

        assertNotNull(ack);
        assertFalse(scheduleExists(f.stream, schedSubject));
    }

    @Test
    public void testPublishAndCancel_ifExists_whenMissing() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("p3");
        String tgtSubject   = f.tgt("p3");

        PublishAck ack = ScheduleManagement.publishAndCancelSchedule(
            jsm, schedSubject, tgtSubject, "cancel-now".getBytes(), null, true);

        assertNull(ack);
    }

    // -- publishAndCancelSchedule(jsm, sched, seq, tgt, data) ------------------

    @Test
    public void testPublishAndCancel_withSequence_success() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("p4");
        String tgtSubject   = f.tgt("p4");
        long seq = scheduleInTheFuture(schedSubject, tgtSubject, "body");

        PublishAck ack = ScheduleManagement.publishAndCancelSchedule(
            jsm, schedSubject, seq, tgtSubject, "cancel-now".getBytes(), null);

        assertNotNull(ack);
        assertFalse(scheduleExists(f.stream, schedSubject));
    }

    @Test
    public void testPublishAndCancel_withSequence_wrongSequenceFails() throws Exception {
        Fixture f = newFixture();
        String schedSubject = f.sched("p5");
        String tgtSubject   = f.tgt("p5");
        long seq = scheduleInTheFuture(schedSubject, tgtSubject, "body");

        assertThrows(JetStreamApiException.class, () ->
            ScheduleManagement.publishAndCancelSchedule(
                jsm, schedSubject, seq + 999, tgtSubject, "cancel-now".getBytes(), null));
    }
}
