// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;
import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.nats.client.support.NatsJetStreamConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class BatchPublishTests {
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

    private static String createStream(boolean allowAtomic, String... subjects) throws Exception {
        String streamName = NUID.nextGlobalSequence();
        jsm.addStream(StreamConfiguration.builder()
            .name(streamName)
            .subjects(subjects)
            .storageType(StorageType.Memory)
            .allowAtomicPublish(allowAtomic)
            .build());
        return streamName;
    }

    private static BatchPublisher publisher() {
        return BatchPublisher.builder().connection(nc).build();
    }

    private static String nextHeader(Subscription sub, String header) throws Exception {
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        assertNotNull(m, "the core subscriber should have seen the published message");
        assertNotNull(m.getHeaders(), "the message should have headers");
        return m.getHeaders().getFirst(header);
    }

    private static long msgCount(String streamName) throws Exception {
        return jsm.getStreamInfo(streamName).getStreamState().getMsgCount();
    }

    // ----------------------------------------------------------------------------------
    // happy path
    // ----------------------------------------------------------------------------------
    @Test
    public void testCommit() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = publisher();
        bp.add(subject, data("1"));
        bp.add(subject, data("2"));
        PublishAck pa = bp.commit(subject, data("3"));

        // unlike EOB, the message that ends the batch is stored
        assertEquals(3, pa.getBatchSize());
        assertEquals(bp.getBatchId(), pa.getBatchId());
        assertEquals(3, bp.size());
        assertEquals(3, msgCount(streamName));
        assertTrue(bp.isClosed());

        MessageInfo mi = jsm.getMessage(streamName, 3);
        assertNotNull(mi.getHeaders());
        assertEquals(NATS_BATCH_COMMIT_STORE, mi.getHeaders().getFirst(NATS_BATCH_COMMIT_HDR));
        assertEquals("3", new String(mi.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testHeadersOnEveryStoredMessage() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = publisher();
        for (int i = 1; i <= 4; i++) {
            Headers h = new Headers();
            h.put("my-header", "xyz-" + i);
            bp.add(subject, h, data("data-" + i));
        }
        Headers last = new Headers();
        last.put("my-header", "xyz-5");
        bp.commit(subject, last, data("data-5"));

        assertEquals(5, msgCount(streamName));
        for (int seq = 1; seq <= 5; seq++) {
            MessageInfo mi = jsm.getMessage(streamName, seq);
            Headers h = mi.getHeaders();
            assertNotNull(h, "seq " + seq + " should have headers");
            assertEquals(bp.getBatchId(), h.getFirst(NATS_BATCH_ID_HDR), "batch id on seq " + seq);
            assertEquals(Integer.toString(seq), h.getFirst(NATS_BATCH_SEQUENCE_HDR), "batch sequence on seq " + seq);
            assertEquals("xyz-" + seq, h.getFirst("my-header"), "user header on seq " + seq);
            assertEquals("data-" + seq, new String(mi.getData(), StandardCharsets.UTF_8));
            // the commit header belongs only on the final message
            if (seq == 5) {
                assertEquals(NATS_BATCH_COMMIT_STORE, h.getFirst(NATS_BATCH_COMMIT_HDR));
            }
            else {
                assertNull(h.getFirst(NATS_BATCH_COMMIT_HDR), "seq " + seq + " must not carry a commit header");
            }
        }
    }

    @Test
    public void testCommitAsync() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = publisher();
        bp.add(subject, data("1"));
        PublishAck pa = bp.commitAsync(subject, data("2")).get();
        assertEquals(2, pa.getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    // ----------------------------------------------------------------------------------
    // adding
    // ----------------------------------------------------------------------------------
    @Test
    public void testAckFirstAndAckEvery() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // ackEvery makes add() do a round trip every N messages. An unexpected ack payload
        // would throw "Invalid ack returned from add with confirm", so reaching the commit proves
        // the non-commit header path is still right.
        BatchPublisher bp = BatchPublisher.builder()
            .connection(nc)
            .ackFirst(true)
            .ackEvery(3)
            .build();
        assertTrue(bp.ackFirst());
        assertEquals(3, bp.getAckEvery());

        for (int i = 1; i <= 10; i++) {
            bp.add(subject, data("d" + i));
        }
        assertEquals(10, bp.size());
        PublishAck pa = bp.commit(subject, data("d11"));
        assertEquals(11, pa.getBatchSize());
        assertEquals(11, bp.size());
        assertEquals(11, msgCount(streamName));
    }

    @Test
    public void testAckFirstFalse() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        assertFalse(bp.ackFirst());
        bp.add(subject, data("1"));
        bp.add(subject, data("2"));
        // two adds plus the commit message is three stored messages
        assertEquals(3, bp.commit(subject, data("3")).getBatchSize());
        assertEquals(3, bp.size());
        assertEquals(3, msgCount(streamName));
    }

    @Test
    public void testAddAcked() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = publisher();
        bp.addAcked(subject, data("1"));
        bp.addAcked(subject, null, data("2"));
        assertEquals(2, bp.size());
        assertEquals(3, bp.commit(subject, data("3")).getBatchSize());
        assertEquals(3, msgCount(streamName));
    }

    // ----------------------------------------------------------------------------------
    // guards
    // ----------------------------------------------------------------------------------
    @Test
    public void testDiscardAndStateGuards() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublisher bp = publisher();
        assertTrue(bp.isOpen());
        bp.add(subject, data("1"));
        bp.discard();
        assertTrue(bp.isDiscarded());
        assertFalse(bp.isOpen());
        assertThrows(BatchPublishException.class, () -> bp.add(subject, data("2")));
        assertThrows(BatchPublishException.class, () -> bp.addAcked(subject, data("2")));
        assertThrows(BatchPublishException.class, () -> bp.commit(subject, data("2")));
        // a discarded batch was never committed, so nothing is stored
        assertEquals(0, msgCount(streamName));

        BatchPublisher committed = publisher();
        committed.add(subject, data("1"));
        committed.commit(subject, data("2"));
        assertTrue(committed.isClosed());
        assertThrows(BatchPublishException.class, () -> committed.add(subject, data("3")));
    }

    @Test
    public void testExpectedLastSequenceOnlyOnFirstMessage() throws Exception {
        String prefix = NUID.nextGlobalSequence();
        String subjectA = prefix + ".a";
        String subjectB = prefix + ".b";
        String streamName = createStream(true, prefix + ".>");

        BatchPublishOptions expectLastSeq = BatchPublishOptions.builder().expectedLastSequence(0).build();
        BatchPublisher bp = publisher();
        bp.add(subjectA, data("1"), expectLastSeq);

        // ADR-50 allows it only on the first message, and the server kills the whole batch for
        // it at commit time, so the client refuses before anything goes on the wire
        BatchPublishException e = assertThrows(BatchPublishException.class,
            () -> bp.add(subjectA, data("2"), expectLastSeq));
        assertTrue(e.getMessage().contains("Only the first message"), e.getMessage());
        assertThrows(BatchPublishException.class, () -> bp.commit(subjectA, data("2"), expectLastSeq));

        // a rejected call spends no sequence and does not end the batch, so the caller can
        // carry on with corrected options
        assertEquals(1, bp.size());
        assertTrue(bp.isOpen());
        assertEquals(2, bp.commit(subjectA, data("2")).getBatchSize());
        assertEquals(2, msgCount(streamName));

        // the per subject expectation is deliberately not restricted: the server allows it on
        // any message as long as no earlier message in the batch wrote that same subject
        BatchPublisher bp2 = publisher();
        bp2.add(subjectA, data("1"));
        bp2.add(subjectB, data("2"), BatchPublishOptions.builder()
            .expectedLastSubjectSequence(0)
            .expectedLastSubjectSequenceSubject(subjectB)
            .build());
        assertEquals(3, bp2.commit(subjectA, data("3")).getBatchSize());
    }

    @Test
    public void testUserHeadersTheProtocolDoesNotAllow() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        BatchPublisher bp = publisher();

        // the publisher writes these itself, and updateHeaders copies user headers over the top
        // of them, so a caller header of the same name would corrupt the batch
        for (String managed : new String[]{NATS_BATCH_ID_HDR, NATS_BATCH_SEQUENCE_HDR, NATS_BATCH_COMMIT_HDR}) {
            Headers h = new Headers();
            h.put(managed, "anything");
            BatchPublishException e = assertThrows(BatchPublishException.class,
                () -> bp.add(subject, h, data("1")));
            assertTrue(e.getMessage().contains(managed), e.getMessage());
        }

        // the server answers 10177 for this one on any message of a batch, including the first
        Headers lastMsgId = new Headers();
        lastMsgId.put(EXPECTED_LAST_MSG_ID_HDR, "some-id");
        assertThrows(BatchPublishException.class, () -> bp.add(subject, lastMsgId, data("1")));

        // Nats-Msg-Id is allowed: the server supports de-duplication in batches from 2.12.1 and
        // only rejects a duplicate within one batch
        Headers msgId = new Headers();
        msgId.put(MSG_ID_HDR, "id-1");
        bp.add(subject, msgId, data("1"));

        // the first message rule also applies to a raw header, not only to BatchPublishOptions
        Headers expect = new Headers();
        expect.put(EXPECTED_LAST_SEQ_HDR, "0");
        BatchPublishException e = assertThrows(BatchPublishException.class,
            () -> bp.add(subject, expect, data("2")));
        assertTrue(e.getMessage().contains("Only the first message"), e.getMessage());

        // none of the rejections spent a sequence or ended the batch
        assertEquals(1, bp.size());
        assertTrue(bp.isOpen());

        // and on the first message the same raw header is fine
        BatchPublisher first = publisher();
        first.add(subject, expect, data("1"));
        assertEquals(2, first.commit(subject, data("2")).getBatchSize());
    }

    @Test
    public void testConnectionRejectionIsChecked() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        // jnats rejects an invalid subject with an unchecked IllegalArgumentException. An add
        // that fails must fail the same way whatever rejected it, so both send paths wrap it:
        // the acked path goes through request, the plain path through publish.
        BatchPublisher acked = BatchPublisher.builder().connection(nc).build();
        assertThrows(BatchPublishException.class, () -> acked.add("has space", data("1")));

        BatchPublisher plain = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        assertThrows(BatchPublishException.class, () -> plain.add("has space", data("1")));

        // and because nothing left the client, the sequence is given back rather than spent: the
        // batch carries on from where it was, with no hole for the server to reject
        assertEquals(0, acked.size());
        assertEquals(0, plain.size());
        acked.add(subject, data("1"));
        assertEquals(2, acked.commit(subject, data("2")).getBatchSize());

        // the same on a commit that never leaves
        BatchPublisher onCommit = publisher();
        onCommit.add(subject, data("1"));
        assertThrows(BatchPublishException.class, () -> onCommit.commit("has space", data("2")));
        assertEquals(1, onCommit.size());
    }

    @Test
    public void testAtomicDisabledSurfacesOnTheFirstAdd() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(false, subject);

        // with ackFirst on, the first add is a request, so the server's rejection arrives there
        // rather than at the commit. It must carry the server's error, not a generic sentence.
        BatchPublisher bp = BatchPublisher.builder().connection(nc).build();
        BatchPublishException e = assertThrows(BatchPublishException.class, () -> bp.add(subject, data("1")));
        assertEquals(JS_ATOMIC_PUBLISH_DISABLED, e.getApiErrorCode());
        assertNotNull(e.getJsApiException());
        assertTrue(e.getMessage().contains("atomic publish is disabled"), e.getMessage());
    }

    @Test
    public void testBatchSizeLimit() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // the server counts the commit message too, so 1000 adds plus a commit is 1001 and the
        // whole batch is lost at the last step. Nothing guards this locally yet.
        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        for (int i = 1; i <= 1000; i++) {
            bp.add(subject, data("d" + i));
        }
        BatchPublishException e = assertThrows(BatchPublishException.class, () -> bp.commit(subject, data("last")));
        assertEquals(JS_ATOMIC_PUBLISH_TOO_LARGE_BATCH, e.getApiErrorCode());
        assertEquals(0, msgCount(streamName));

        // 999 adds plus the commit is exactly the limit and is accepted
        BatchPublisher ok = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        for (int i = 1; i <= 999; i++) {
            ok.add(subject, data("d" + i));
        }
        assertEquals(1000, ok.commit(subject, data("last")).getBatchSize());
        assertEquals(1000, msgCount(streamName));
    }

    @Test
    public void testMessageTtlAppliedAndPrecedence() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = NUID.nextGlobalSequence();
        jsm.addStream(StreamConfiguration.builder()
            .name(streamName)
            .subjects(subject)
            .storageType(StorageType.Memory)
            .allowAtomicPublish(true)
            .allowMessageTtl(true)
            .build());

        // the server consumes the ttl header and strips it before storing
        // (nats-server/server/stream.go:7253), so read it off the wire rather than out of the
        // stream. A core subscriber sees exactly the header block the publisher built.
        Subscription sub = nc.subscribe(subject);

        // the publisher ttl applies to a message with no options, and an options ttl wins over it
        BatchPublisher bp = BatchPublisher.builder().connection(nc).messageTtlSeconds(60).build();
        bp.add(subject, data("publisher ttl"));
        bp.add(subject, data("options ttl"), BatchPublishOptions.builder().messageTtlSeconds(30).build());
        bp.commit(subject, data("commit ttl"), BatchPublishOptions.builder().messageTtlNever().build());

        assertEquals("60s", nextHeader(sub, MSG_TTL_HDR));
        assertEquals("30s", nextHeader(sub, MSG_TTL_HDR));
        assertEquals("never", nextHeader(sub, MSG_TTL_HDR));
        sub.unsubscribe();
        assertEquals(3, msgCount(streamName));
    }

    @Test
    public void testTerminalOperationsAreIdempotent() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        BatchPublisher bp = publisher();
        bp.add(subject, data("1"));
        bp.discard();
        bp.discard();
        assertTrue(bp.isDiscarded());
        assertFalse(bp.isOpen());

        // discarding a committed batch does not rewrite what happened to it
        BatchPublisher committed = publisher();
        committed.add(subject, data("1"));
        committed.commit(subject, data("2"));
        assertTrue(committed.isClosed());
        committed.discard();
        assertTrue(committed.isDiscarded(), "discard after commit is allowed and does change the state");
    }

    @Test
    public void testCommitAsyncFailureCarriesTheCause() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);
        BatchPublisher seed = publisher();
        seed.add(subject, data("1"));
        seed.commit(subject, data("2"));

        // the future completes with the BatchPublishException itself, not a RuntimeException
        // wrapping it, so the caller unwraps one layer rather than two
        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"), BatchPublishOptions.builder().expectedLastSequence(1).build());
        ExecutionException ee = assertThrows(ExecutionException.class,
            () -> bp.commitAsync(subject, data("2")).get(5, TimeUnit.SECONDS));
        assertInstanceOf(BatchPublishException.class, ee.getCause());
        assertEquals(JS_WRONG_LAST_SEQUENCE, ((BatchPublishException)ee.getCause()).getApiErrorCode());
    }

    @Test
    public void testAtomicDisabled() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(false, subject);

        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"));
        BatchPublishException e = assertThrows(BatchPublishException.class, () -> bp.commit(subject, data("2")));
        assertEquals(JS_ATOMIC_PUBLISH_DISABLED, e.getApiErrorCode());
    }

    @Test
    public void testExpectationsEnforcedOnCommit() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        BatchPublishOptions opts = BatchPublishOptions.builder().expectedLastSequence(999).build();
        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"), opts);
        bp.add(subject, data("2"));
        assertThrows(BatchPublishException.class, () -> bp.commit(subject, data("3")));
        assertEquals(0, msgCount(streamName));
    }

    @Test
    public void testExpectationsSatisfied() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // a correct expectation must still let the batch through
        BatchPublishOptions opts = BatchPublishOptions.builder().expectedLastSequence(0).build();
        BatchPublisher bp = BatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"), opts);
        assertEquals(2, bp.commit(subject, data("2")).getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    @Test
    public void testMessageTtl() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = NUID.nextGlobalSequence();
        jsm.addStream(StreamConfiguration.builder()
            .name(streamName)
            .subjects(subject)
            .storageType(StorageType.Memory)
            .allowAtomicPublish(true)
            .allowMessageTtl(true)
            .build());

        // the TTL travels in the same header block updateHeaders builds
        BatchPublishOptions opts = BatchPublishOptions.builder().messageTtlSeconds(60).build();
        BatchPublisher bp = publisher();
        bp.add(subject, data("1"), opts);
        assertEquals(2, bp.commit(subject, null, data("2"), opts).getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    // ----------------------------------------------------------------------------------
    // builder
    // ----------------------------------------------------------------------------------
    @Test
    public void testBatchIdGeneratedAndValidated() {
        BatchPublisher generated = publisher();
        assertNotNull(generated.getBatchId());
        assertFalse(generated.getBatchId().isEmpty());

        BatchPublisher explicit = BatchPublisher.builder().connection(nc).batchId("my-batch-id").build();
        assertEquals("my-batch-id", explicit.getBatchId());

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            sb.append("x");
        }
        assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(nc).batchId(sb.toString()).build());

        // the id must be a single subject token, because the fast publishers put it in the
        // reply subject and the same rule is applied to both families
        assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(nc).batchId("has.dot").build());
        assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(nc).batchId("has space").build());
        assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(nc).batchId("wild*card").build());
        assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(nc).batchId("gt>").build());

        assertThrows(IllegalArgumentException.class, () -> BatchPublisher.builder().build());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeprecatedDurationAckTimeoutConverts() {
        // 0.2.2 shipped the Duration setter, so removing it would have been a NoSuchMethodError
        // for anyone who swapped the jar without recompiling. It converts and floors instead.
        assertEquals(Duration.ofSeconds(3),
            BatchPublisher.builder().connection(nc).ackTimeout(Duration.ofSeconds(3)).build().getAckTimeout());

        // sub millisecond and zero become the default rather than an unbounded or instant wait
        Duration dflt = BatchPublisher.builder().connection(nc).build().getAckTimeout();
        assertEquals(dflt,
            BatchPublisher.builder().connection(nc).ackTimeout(Duration.ofNanos(500)).build().getAckTimeout());
        assertEquals(dflt,
            BatchPublisher.builder().connection(nc).ackTimeout(Duration.ZERO).build().getAckTimeout());
        assertEquals(dflt,
            BatchPublisher.builder().connection(nc).ackTimeout(Duration.ofSeconds(-5)).build().getAckTimeout());
    }

    @Test
    public void testSharedBuilderSettingsApply() {
        // the settings live on the shared base builder, so they must survive the self typing
        BatchPublisher bp = BatchPublisher.builder()
            .connection(nc)
            .batchId("my-batch")
            .ackFirst(false)
            .ackEvery(5)
            .messageTtlSeconds(30)
            .ackTimeout(3000)
            .build();

        assertEquals("my-batch", bp.getBatchId());
        assertFalse(bp.ackFirst());
        assertEquals(5, bp.getAckEvery());
        assertEquals("30s", bp.getMessageTtl());
        assertEquals(Duration.ofSeconds(3), bp.getAckTimeout());
    }
}
