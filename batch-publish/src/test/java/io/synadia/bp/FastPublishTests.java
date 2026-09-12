// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static io.nats.client.support.NatsJetStreamConstants.EXPECTED_LAST_SEQ_HDR;
import static io.nats.client.support.NatsJetStreamConstants.JS_BATCH_PUBLISH_DISABLED;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Gap injection under GapMode.Fail, meaning a real dropped message or a leader change, is not
 * reliably reproducible against a single server, so it is not tested here. Cover it manually
 * against a cluster.
 */
public class FastPublishTests {
    static NatsServerRunner runner;
    static Connection nc;
    static JetStreamManagement jsm;

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

    private static String createStream(boolean allowBatched, String... subjects) throws Exception {
        String streamName = NUID.nextGlobalSequence();
        jsm.addStream(StreamConfiguration.builder()
            .name(streamName)
            .subjects(subjects)
            .storageType(StorageType.Memory)
            .allowBatched(allowBatched)
            .build());
        return streamName;
    }

    private static long msgCount(String streamName) throws Exception {
        return jsm.getStreamInfo(streamName).getStreamState().getMsgCount();
    }

    private static FastPublisher.Builder builder() {
        return FastPublisher.builder().connection(nc).ackTimeout(10_000);
    }

    private static EobFastPublisher.Builder eobBuilder() {
        return EobFastPublisher.builder().connection(nc).ackTimeout(10_000);
    }

    // ----------------------------------------------------------------------------------
    // happy paths
    // ----------------------------------------------------------------------------------
    @Test
    public void testFastPublishGapOk() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        FastPublisher fp = builder().gapMode(GapMode.Ok).build();
        for (int i = 1; i <= 1000; i++) {
            fp.add(subject, data("data-" + i));
        }
        PublishAck pa = fp.commit(subject, data("last"));

        assertEquals(1001, pa.getBatchSize());
        assertEquals(fp.getBatchId(), pa.getBatchId());
        assertEquals(1001, msgCount(streamName));
        assertTrue(fp.isTerminal());
        assertEquals(0, fp.gapCount());
        assertNull(fp.getLastGap());
    }

    @Test
    public void testFastPublishGapFailEndingInEob() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        EobFastPublisher fp = eobBuilder().gapMode(GapMode.Fail).build();
        for (int i = 1; i <= 100; i++) {
            fp.add(subject, data("data-" + i));
        }
        PublishAck pa = fp.commit();

        // the EOB marker consumed a batch sequence but is not counted and not stored
        assertEquals(100, pa.getBatchSize());
        assertEquals(100, msgCount(streamName));
        assertTrue(fp.isTerminal());
        assertEquals(0, fp.gapCount());
        assertNull(fp.getLastGap());
    }

    @Test
    public void testSingleMessageBatch() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // ADR-50 carves this out as returning a plain PubAck with no preceding flow ack
        FastPublisher fp = builder().build();
        fp.add(subject, data("one"));
        PublishAck pa = fp.commit(subject, data("two"));

        assertEquals(2, pa.getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    @Test
    public void testSizeMatchesBatchSize() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamNameEob = createStream(true, subject);

        // ADR-50: "The pub ack's BatchSize will reflect the messages in the batch, without
        // counting the EOB message." size() must agree with the server, not count the sentinel.
        EobFastPublisher eob = eobBuilder().build();
        for (int i = 1; i <= 10; i++) {
            eob.add(subject, data("d" + i));
        }
        PublishAck eobAck = eob.commit();
        assertEquals(10, eobAck.getBatchSize());
        assertEquals(10, eob.size(), "size() must exclude the EOB sentinel");
        assertEquals(eobAck.getBatchSize(), eob.size());
        assertEquals(10, msgCount(streamNameEob));

        // a commit that stores its final message does count that message
        String subject2 = NUID.nextGlobalSequence();
        String streamNameStore = createStream(true, subject2);
        FastPublisher stored = builder().build();
        for (int i = 1; i <= 10; i++) {
            stored.add(subject2, data("d" + i));
        }
        PublishAck storeAck = stored.commit(subject2, data("final"));
        assertEquals(11, storeAck.getBatchSize());
        assertEquals(11, stored.size(), "size() must count a stored final message");
        assertEquals(storeAck.getBatchSize(), stored.size());
        assertEquals(11, msgCount(streamNameStore));
    }

    // ----------------------------------------------------------------------------------
    // flow control
    // ----------------------------------------------------------------------------------
    @Test
    public void testFlowControlObserved() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        List<Long> flowChanges = new ArrayList<>();
        EobFastPublisher fp = eobBuilder()
            .maxFlow(100)
            .listener(new FastPublishListener() {
                @Override
                public void onFlowChange(long ackEvery) {
                    flowChanges.add(ackEvery);
                }
            })
            .build();

        for (int i = 1; i <= 2000; i++) {
            fp.add(subject, data("data-" + i));
        }
        fp.commit();

        assertFalse(flowChanges.isEmpty(), "the server must report a starting flow rate");
        assertTrue(fp.flow() > 0);
        // the server ramps up toward, but never past, what we asked for
        for (Long f : flowChanges) {
            assertTrue(f <= 100, "flow " + f + " exceeded the requested maximum");
        }
    }

    @Test
    public void testFlowControlRespected() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        EobFastPublisher fp = eobBuilder().maxOutstandingAcks(1).maxFlow(10).build();
        for (int i = 1; i <= 500; i++) {
            FastPubAck a = fp.add(subject, data("data-" + i));
            long outstanding = a.getBatchSequence() - a.getAckSequence();
            // with 1 outstanding ack allowed we may never be more than one full window ahead
            assertTrue(outstanding <= fp.flow() * 2,
                "outstanding " + outstanding + " exceeded twice the flow " + fp.flow());
        }
        fp.commit();
    }

    @Test
    public void testPingDoesNotAdvanceSequence() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        EobFastPublisher fp = eobBuilder().build();
        fp.add(subject, data("1"));
        fp.add(subject, data("2"));
        long before = fp.size();
        fp.ping();
        assertEquals(before, fp.size(), "ping must not consume a batch sequence");

        PublishAck pa = fp.commit();
        assertEquals(2, pa.getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    // ----------------------------------------------------------------------------------
    // errors and guards
    // ----------------------------------------------------------------------------------
    @Test
    public void testBatchedDisabled() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(false, subject);

        // feature detection happens on the first add, which is where the server's answer lives
        FastPublisher fp = builder().build();
        FastPublishException e = assertThrows(FastPublishException.class, () -> fp.add(subject, data("1")));
        assertEquals(JS_BATCH_PUBLISH_DISABLED, e.getApiErrorCode());
    }

    @Test
    public void testHeaderCheckFailureGapOk() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        AtomicLong errorSeq = new AtomicLong(-1);
        EobFastPublisher fp = eobBuilder()
            .gapMode(GapMode.Ok)
            .listener(new FastPublishListener() {
                @Override
                public void onError(FastFlowError error) {
                    errorSeq.set(error.getSequence());
                }
            })
            .build();

        for (int i = 1; i <= 10; i++) {
            if (i == 5) {
                Headers h = new Headers();
                h.put(EXPECTED_LAST_SEQ_HDR, "9999"); // wrong on purpose
                fp.add(subject, h, data("data-" + i));
            }
            else {
                fp.add(subject, data("data-" + i));
            }
        }
        fp.commit();

        // in Ok mode the failure is reported and the batch keeps going
        assertNotEquals(-1, errorSeq.get(), "onError should have fired for the bad message");
    }

    @Test
    public void testCommitEobEmptyBatch() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        EobFastPublisher fp = eobBuilder().build();
        FastPublishException e = assertThrows(FastPublishException.class, fp::commit);
        assertTrue(e.getMessage().contains("Cannot commit an empty batch"), e.getMessage());
    }

    @Test
    public void testAbandon() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher fp = builder().build();
        fp.add(subject, data("1"));
        fp.abandon();

        assertTrue(fp.isTerminal());
        assertThrows(FastPublishException.class, () -> fp.add(subject, data("2")));
        assertThrows(FastPublishException.class, () -> fp.commit(subject, data("2")));
    }

    @Test
    public void testCommitAsTheFirstCall() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // legal: a one message batch. The server's parser rejects only an EOB at sequence 1, and
        // the first-reply feature check lives in add, so this path skips it and learns about a
        // server problem from the PublishAck instead.
        FastPublisher fp = builder().build();
        PublishAck pa = fp.commit(subject, data("only"));
        assertEquals(1, pa.getBatchSize());
        assertEquals(1, msgCount(streamName));
        assertEquals(EndReason.Committed, fp.getEndReason());

        // the EOB commit is the one that cannot start a batch, and it is refused locally
        EobFastPublisher eob = eobBuilder().build();
        assertThrows(FastPublishException.class, eob::commit);
    }

    @Test
    public void testTerminalOperationsAreIdempotent() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher fp = builder().build();
        fp.add(subject, data("1"));
        fp.abandon();
        fp.abandon();
        fp.close();
        assertTrue(fp.isTerminal());
        assertEquals(EndReason.Abandoned, fp.getEndReason());

        // every operation is refused once the batch is over, including ping
        assertThrows(FastPublishException.class, () -> fp.add(subject, data("2")));
        assertThrows(FastPublishException.class, () -> fp.commit(subject, data("2")));
        assertThrows(FastPublishException.class, fp::ping);
    }

    @Test
    public void testCloseAbandonsNeverCommits() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher escaped;
        try (FastPublisher fp = builder().build()) {
            escaped = fp;
            fp.add(subject, data("1"));
            assertFalse(fp.isTerminal());
        }

        // close() is abandon(): the batch ends unusable and no PublishAck was ever produced.
        // Whatever the server already persisted stays persisted, which is fast ingest, not a
        // property of close.
        assertTrue(escaped.isTerminal());
        assertThrows(FastPublishException.class, () -> escaped.add(subject, data("2")));

        // and it is harmless after the batch has already ended on its own
        FastPublisher committed = builder().build();
        committed.add(subject, data("1"));
        assertNotNull(committed.commit(subject, data("2")));
        committed.close();
        assertTrue(committed.isTerminal());
    }

    @Test
    public void testBatchIdTooLong() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            sb.append("x");
        }
        assertThrows(IllegalArgumentException.class, () -> builder().batchId(sb.toString()).build());
    }

    @Test
    public void testBatchIdInvalidCharacters() {
        // the id is one token of the reply subject, which the server parses right to left. A dot
        // would leave the server reading only "dot" as the id while the client reported
        // "has.dot", and a wildcard or a space makes the subject itself invalid.
        assertThrows(IllegalArgumentException.class, () -> builder().batchId("has.dot").build());
        assertThrows(IllegalArgumentException.class, () -> builder().batchId("has space").build());
        assertThrows(IllegalArgumentException.class, () -> builder().batchId("wild*card").build());
        assertThrows(IllegalArgumentException.class, () -> builder().batchId("gt>").build());
    }

    @Test
    public void testAckTimeoutNotInfinite() {
        // jnats reads a duration under one nanosecond as wait forever, so a zero or negative ack
        // timeout must fall back to the connection timeout rather than disabling every timeout in
        // the publisher. Nothing captures this subject, so nothing ever answers the $FI reply and
        // the add can only end by giving up.
        for (long millis : new long[]{0, -5000}) {
            FastPublisher fp = FastPublisher.builder().connection(nc).ackTimeout(millis).build();
            assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThrows(FastPublishException.class, () -> fp.add(NUID.nextGlobalSequence(), data("1"))));
        }
    }

    @Test
    public void testConnectionRejectionIsChecked() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        // jnats rejects an invalid subject with an unchecked IllegalArgumentException, and an add
        // that fails must fail the same way whatever rejected it
        FastPublisher fp = builder().build();
        assertThrows(FastPublishException.class, () -> fp.add("has space", data("1")));

        // nothing left the client, so the sequence is given back and the batch is still coherent:
        // the next add is sequence 1 and the batch commits as a two message batch
        assertEquals(0, fp.size());
        fp.add(subject, data("1"));
        assertEquals(1, fp.size());
        assertEquals(2, fp.commit(subject, data("2")).getBatchSize());
    }

    /**
     * The flow and gap tokens are stated to the server in the reply subject
     * {@code <inbox>.<batchId>.<flow>.<gap>.<seq>.<op>.$FI}, so a core subscriber can read back
     * exactly what the builder decided. Counting from the end because the inbox contains dots.
     */
    private static String replyToken(String reply, int fromEnd) {
        String[] parts = reply.split("\\.");
        return parts[parts.length - fromEnd];
    }

    private static Message publishOneAndCaptureReply(FastPublisher fp, String subject) throws Exception {
        Subscription sub = nc.subscribe(subject);
        fp.add(subject, data("1"));
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        assertNotNull(m, "the core subscriber should have seen the published message");
        sub.unsubscribe();
        return m;
    }

    @Test
    public void testMaxFlowStatedToServer() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        // less than 1 means "use the default", matching the Go implementation, rather than
        // clamping to 1 which would silently be the slowest possible setting
        FastPublisher zero = builder().maxFlow(0).build();
        assertEquals(Integer.toString(FastPublisher.DEFAULT_MAX_FLOW),
            replyToken(publishOneAndCaptureReply(zero, subject).getReplyTo(), 5));
        zero.abandon();

        FastPublisher explicit = builder().maxFlow(250).build();
        assertEquals("250", replyToken(publishOneAndCaptureReply(explicit, subject).getReplyTo(), 5));
        explicit.abandon();

        // the server reads the token as a uint16, so it is capped rather than overflowing
        FastPublisher huge = builder().maxFlow(999999).build();
        assertEquals(Integer.toString(FastPublisher.MAX_FLOW_CEILING),
            replyToken(publishOneAndCaptureReply(huge, subject).getReplyTo(), 5));
        huge.abandon();
    }

    @Test
    public void testGapModeStatedToServer() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher fail = builder().gapMode(GapMode.Fail).build();
        assertEquals("fail", replyToken(publishOneAndCaptureReply(fail, subject).getReplyTo(), 4));
        fail.abandon();

        FastPublisher ok = builder().gapMode(GapMode.Ok).build();
        assertEquals("ok", replyToken(publishOneAndCaptureReply(ok, subject).getReplyTo(), 4));
        ok.abandon();

        // null falls back to the safe default rather than throwing
        assertEquals(GapMode.Fail, builder().gapMode(null).build().getGapMode());
    }

    /**
     * The publisher's control channel is {@code <inbox>.<batchId>.>} while the reply subject it
     * publishes with is {@code <inbox>.<batchId>.<flow>.<gap>.<seq>.<op>.$FI}, so dropping the
     * last five tokens of a captured reply gives a subject the publisher is subscribed to.
     */
    private static String controlSubject(String reply) {
        int cut = reply.length();
        for (int i = 0; i < 5; i++) {
            cut = reply.lastIndexOf('.', cut - 1);
        }
        return reply.substring(0, cut);
    }

    private static void injectGap(String control, long lastSeq, long seq) throws Exception {
        nc.publish(control + ".gap",
            data("{\"type\":\"gap\",\"last_seq\":" + lastSeq + ",\"seq\":" + seq + "}"));
        nc.flush(Duration.ofSeconds(2));
    }

    private static void injectPubAck(String control, String stream, long seq, String batchId, long count) throws Exception {
        // a control message with no "type" field is the terminal PublishAck
        nc.publish(control + ".ack",
            data("{\"stream\":\"" + stream + "\",\"seq\":" + seq
                + ",\"batch\":\"" + batchId + "\",\"count\":" + count + "}"));
        nc.flush(Duration.ofSeconds(2));
    }

    private static void injectError(String control, long seq, int errCode, String description) throws Exception {
        nc.publish(control + ".err",
            data("{\"type\":\"err\",\"seq\":" + seq + ",\"error\":{\"code\":400,\"err_code\":" + errCode
                + ",\"description\":\"" + description + "\"}}"));
        nc.flush(Duration.ofSeconds(2));
    }

    /**
     * The control channel is read on the publisher's own dispatcher thread, so an injected
     * message that ends the batch lands a moment after the publish that injected it. Tests that
     * assert on what happens *after* the batch is known to be over have to wait for that, which
     * is itself the new behavior: the publisher knows without being asked.
     */
    private static void awaitTerminal(FastPublisher fp) throws Exception {
        for (int i = 0; i < 200 && !fp.isTerminal(); i++) {
            //noinspection BusyWait
            Thread.sleep(5);
        }
        assertTrue(fp.isTerminal(), "the dispatcher should have classified the injected message");
    }

    /**
     * Control messages are only read when the publisher publishes, so keep adding until the
     * injected gap has been drained. One add is almost always enough. In {@link GapMode#Fail}
     * that add throws rather than returning, because the drain at the front of add ends the
     * batch before a sequence is spent on the message, so the throw is the expected outcome.
     */
    private static void addUntilGapSeen(FastPublisher fp, String subject, long gaps) throws Exception {
        for (int i = 0; i < 100 && fp.gapCount() < gaps; i++) {
            try {
                fp.add(subject, data("drain-" + i));
            }
            catch (FastPublishException e) {
                if (fp.isTerminal()) {
                    return;
                }
                throw e;
            }
        }
    }

    /**
     * Gap accounting, driven by gap frames published onto the publisher's own control channel
     * rather than by a real dropped message, which needs a cluster. Parsing and accounting is
     * all {@code gapCount()} and {@code getLastGap()} are, and that is what this covers.
     */
    @Test
    public void testGapAccounting() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        AtomicLong delivered = new AtomicLong();
        FastPublisher ok = builder()
            .gapMode(GapMode.Ok)
            .listener(new FastPublishListener() {
                @Override
                public void onGap(FastFlowGap gap) {
                    delivered.incrementAndGet();
                }
            })
            .build();

        assertEquals(0, ok.gapCount());
        assertNull(ok.getLastGap());

        String control = controlSubject(publishOneAndCaptureReply(ok, subject).getReplyTo());

        injectGap(control, 10, 15);
        addUntilGapSeen(ok, subject, 1);
        assertEquals(1, ok.gapCount());
        assertNotNull(ok.getLastGap());
        assertEquals(10, ok.getLastGap().getLastSequence());
        assertEquals(15, ok.getLastGap().getSequence());

        // Ok mode keeps going, so a second gap is counted and the later one replaces the first
        injectGap(control, 40, 44);
        addUntilGapSeen(ok, subject, 2);
        assertEquals(2, ok.gapCount());
        assertEquals(40, ok.getLastGap().getLastSequence());
        assertEquals(44, ok.getLastGap().getSequence());
        assertEquals(2, delivered.get());
        assertFalse(ok.isTerminal());

        // the injected gaps are invisible to the server, so the batch commits normally
        PublishAck pa = ok.commit(subject, data("last"));
        assertEquals(ok.size(), pa.getBatchSize());
        assertEquals(ok.size(), msgCount(streamName));

        // Fail mode ends the batch on the first gap, so the count can never go past 1
        FastPublisher fail = builder().gapMode(GapMode.Fail).build();
        control = controlSubject(publishOneAndCaptureReply(fail, subject).getReplyTo());
        long sizeBeforeGap = fail.size();
        injectGap(control, 1, 3);
        awaitTerminal(fail);
        addUntilGapSeen(fail, subject, 1);
        assertEquals(1, fail.gapCount());
        assertEquals(1, fail.getLastGap().getLastSequence());
        assertEquals(3, fail.getLastGap().getSequence());
        assertTrue(fail.isTerminal());

        // the batch was already over before the add ran, so the gap cost no batch sequence and
        // every later call is refused
        assertEquals(sizeBeforeGap, fail.size());
        assertThrows(FastPublishException.class, () -> fail.add(subject, data("after")));
        fail.abandon();
    }

    @Test
    public void testEndReasonSaysWhyTheBatchEnded() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher committed = builder().build();
        assertEquals(EndReason.Open, committed.getEndReason());
        committed.add(subject, data("1"));
        assertEquals(EndReason.Open, committed.getEndReason());
        committed.commit(subject, data("2"));
        assertEquals(EndReason.Committed, committed.getEndReason());

        FastPublisher abandoned = builder().build();
        abandoned.add(subject, data("1"));
        abandoned.abandon();
        assertEquals(EndReason.Abandoned, abandoned.getEndReason());

        // close() is abandon(), so it reports the same ending
        FastPublisher closed = builder().build();
        closed.add(subject, data("1"));
        closed.close();
        assertEquals(EndReason.Abandoned, closed.getEndReason());

        // a gap in Fail mode ends the batch on the server, not on the client's say so
        FastPublisher gapped = builder().gapMode(GapMode.Fail).build();
        String control = controlSubject(publishOneAndCaptureReply(gapped, subject).getReplyTo());
        injectGap(control, 1, 3);
        addUntilGapSeen(gapped, subject, 1);
        assertEquals(EndReason.Gap, gapped.getEndReason());
        gapped.abandon();
        assertEquals(EndReason.Gap, gapped.getEndReason(), "abandon must not overwrite why it really ended");

        // and a per message header check failure does the same in Fail mode
        FastPublisher errored = builder().gapMode(GapMode.Fail).build();
        control = controlSubject(publishOneAndCaptureReply(errored, subject).getReplyTo());
        injectError(control, 2, 10071, "wrong last sequence: 1");
        awaitTerminal(errored);

        // the add drains the error first, which is what fires the listener and records it, and
        // then refuses to publish into a batch that is over
        assertThrows(FastPublishException.class, () -> errored.add(subject, data("after")));
        assertEquals(EndReason.Error, errored.getEndReason());
        assertNotNull(errored.getPendingError());
        assertEquals(10071, errored.getPendingError().getApiErrorCode());
    }

    @Test
    public void testTheBatchIsKnownDeadWhileIdle() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        FastPublisher fp = builder().gapMode(GapMode.Fail).build();
        String control = controlSubject(publishOneAndCaptureReply(fp, subject).getReplyTo());
        assertFalse(fp.isTerminal());

        // nothing is published from here on. The publisher's own dispatcher thread classifies the
        // gap, so the application learns the batch is over without asking and without pinging.
        injectGap(control, 1, 3);
        awaitTerminal(fp);
        assertEquals(EndReason.Gap, fp.getEndReason());

        // the accounting deliberately did not happen there: counters and listener callbacks stay
        // on the caller's thread, and run when it next looks
        assertEquals(0, fp.gapCount());
        assertThrows(FastPublishException.class, () -> fp.add(subject, data("x")));
        assertEquals(1, fp.gapCount());
        assertNotNull(fp.getLastGap());
    }

    @Test
    public void testTerminalAckOfAGapEndedBatchIsReachable() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        FastPublisher fp = builder().gapMode(GapMode.Fail).build();
        String control = controlSubject(publishOneAndCaptureReply(fp, subject).getReplyTo());

        // the server ends a Fail batch on a gap and then sends the batch's final PublishAck,
        // which ADR-50 makes the only authoritative statement of what was persisted
        injectGap(control, 1, 3);
        injectPubAck(control, streamName, 7, fp.getBatchId(), 2);
        addUntilGapSeen(fp, subject, 1);
        assertEquals(EndReason.Gap, fp.getEndReason());

        // committing a batch the server already ended cannot publish anything, but it must hand
        // back that ack rather than refusing empty handed
        FastPublishException e = assertThrows(FastPublishException.class, () -> fp.commit(subject, data("x")));
        assertNotNull(e.getPublishAck(), "the terminal ack must be collected, not discarded");
        assertEquals(2, e.getPublishAck().getBatchSize());
        assertEquals(streamName, e.getPublishAck().getStream());

        // and collecting it does not relabel how the batch ended
        assertEquals(EndReason.Gap, fp.getEndReason());
    }

    @Test
    public void testTerminalAckAbsentWhenTheServerSendsNone() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        // same path, but nothing ever answers. ADR-50 makes these acks best effort, so the
        // commit still fails and simply carries no ack rather than hanging or throwing twice.
        FastPublisher fp = FastPublisher.builder().connection(nc).gapMode(GapMode.Fail).ackTimeout(500).build();
        String control = controlSubject(publishOneAndCaptureReply(fp, subject).getReplyTo());
        injectGap(control, 1, 3);
        addUntilGapSeen(fp, subject, 1);

        FastPublishException e = assertThrows(FastPublishException.class, () -> fp.commit(subject, data("x")));
        assertNull(e.getPublishAck());
        assertEquals(EndReason.Gap, fp.getEndReason());
    }

    @Test
    public void testAckCountIsValidatedExceptAfterAGap() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // on a clean batch the server's count must match the client's, so an ack claiming a
        // different number is a failure rather than something to return
        FastPublisher fp = builder().build();
        String control = controlSubject(publishOneAndCaptureReply(fp, subject).getReplyTo());
        fp.add(subject, data("2"));
        injectPubAck(control, streamName, 9, fp.getBatchId(), 99);
        FastPublishException e = assertThrows(FastPublishException.class, () -> fp.commit(subject, data("3")));
        assertTrue(e.getMessage().contains("99"), e.getMessage());

        // after a gap the client's count is an upper bound rather than an equal, since the
        // server received less than was sent, so the same mismatch must not be treated as one
        FastPublisher gapped = builder().gapMode(GapMode.Fail).build();
        control = controlSubject(publishOneAndCaptureReply(gapped, subject).getReplyTo());
        injectGap(control, 1, 3);
        injectPubAck(control, streamName, 9, gapped.getBatchId(), 1);
        addUntilGapSeen(gapped, subject, 1);
        FastPublishException gapEnd = assertThrows(FastPublishException.class, () -> gapped.commit(subject, data("x")));
        assertNotNull(gapEnd.getPublishAck(), "the terminal ack must survive, not be rejected for its count");
        assertEquals(1, gapEnd.getPublishAck().getBatchSize());
    }

    @Test
    public void testPingUsesTheFirstSubject() throws Exception {
        String first = NUID.nextGlobalSequence();
        String second = NUID.nextGlobalSequence();
        createStream(true, first, second);

        FastPublisher fp = builder().build();
        fp.add(first, data("1"));
        fp.add(second, data("2"));

        // every other client pings the first subject of the batch, and so does this one
        Subscription sub = nc.subscribe(first);
        fp.ping();
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        assertNotNull(m, "the ping should have gone to the first subject");
        sub.unsubscribe();

        // and a batch with no messages has no subject to ping
        FastPublisher empty = builder().build();
        FastPublishException e = assertThrows(FastPublishException.class, empty::ping);
        assertTrue(e.getMessage().contains("no messages"), e.getMessage());
    }

    @Test
    public void testOutstandingAcksClamped() {
        // clamped into MIN..MAX rather than throwing
        assertNotNull(builder().maxOutstandingAcks(0).build());
        assertNotNull(builder().maxOutstandingAcks(99).build());
        assertEquals(2, FastPublisher.DEFAULT_MAX_OUTSTANDING_ACKS);
        assertEquals(3, FastPublisher.MAX_OUTSTANDING_ACKS);
    }
}
