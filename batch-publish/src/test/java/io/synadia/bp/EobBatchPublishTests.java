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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static io.nats.client.support.NatsJetStreamConstants.JS_ATOMIC_PUBLISH_DISABLED;
import static io.nats.client.support.NatsJetStreamConstants.NATS_BATCH_COMMIT_EOB;
import static io.nats.client.support.NatsJetStreamConstants.NATS_BATCH_COMMIT_HDR;
import static io.nats.client.support.NatsJetStreamConstants.NATS_BATCH_COMMIT_STORE;
import static org.junit.jupiter.api.Assertions.*;

public class EobBatchPublishTests {
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

    private static EobBatchPublisher publisher() {
        return EobBatchPublisher.builder().connection(nc).build();
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

        EobBatchPublisher bp = publisher();
        bp.add(subject, data("1"));
        bp.add(subject, data("2"));
        bp.add(subject, data("3"));
        PublishAck pa = bp.commit();

        // the sentinel consumed a batch sequence but must not be counted or stored
        assertEquals(3, pa.getBatchSize());
        assertEquals(bp.getBatchId(), pa.getBatchId());
        assertEquals(3, bp.size());
        assertEquals(3, msgCount(streamName));
        assertTrue(bp.isClosed());

        // the real proof that EOB worked rather than an ordinary commit: the server rewrote
        // the header of the previously received last message from nothing to "1".
        MessageInfo mi = jsm.getMessage(streamName, 3);
        assertNotNull(mi.getHeaders());
        assertEquals(NATS_BATCH_COMMIT_STORE, mi.getHeaders().getFirst(NATS_BATCH_COMMIT_HDR));
        assertEquals("3", new String(mi.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void testSentinelUsesFirstSubject() throws Exception {
        String prefix = NUID.nextGlobalSequence();
        String subjectA = prefix + ".a";
        String subjectB = prefix + ".b";
        createStream(true, prefix + ".*");

        // The sentinel is never stored, so the stream cannot show us where it went. A plain core
        // subscriber can: the sentinel is still published, it is just not persisted.
        Subscription sub = nc.subscribe(prefix + ".*");

        EobBatchPublisher bp = publisher();
        bp.add(subjectA, data("1"));   // first  -> this subject must carry the sentinel
        bp.add(subjectB, data("2"));
        bp.add(subjectB, data("3"));   // last   -> not the sentinel subject
        bp.commit();

        List<String> eobSubjects = new ArrayList<>();
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        while (m != null) {
            if (m.getHeaders() != null
                && NATS_BATCH_COMMIT_EOB.equals(m.getHeaders().getFirst(NATS_BATCH_COMMIT_HDR)))
            {
                eobSubjects.add(m.getSubject());
            }
            m = sub.nextMessage(Duration.ofMillis(200));
        }

        assertEquals(1, eobSubjects.size(), "exactly one EOB sentinel should have been published");
        assertEquals(subjectA, eobSubjects.get(0), "the sentinel must go to the first subject added");
    }

    @Test
    public void testCommitAsync() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        EobBatchPublisher bp = publisher();
        bp.add(subject, data("1"));
        bp.add(subject, data("2"));
        PublishAck pa = bp.commitAsync().get();

        assertEquals(2, pa.getBatchSize());
        assertEquals(2, msgCount(streamName));
    }

    // ----------------------------------------------------------------------------------
    // guards
    // ----------------------------------------------------------------------------------
    @Test
    public void testCommitEmptyBatch() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        // A batch cannot be only a sentinel. Checked locally, matching the other clients, and
        // there is no subject overload that could get around it.
        EobBatchPublisher bp = publisher();
        BatchPublishException e = assertThrows(BatchPublishException.class, bp::commit);
        assertTrue(e.getMessage().contains("Cannot commit an empty batch"), e.getMessage());
        assertTrue(bp.isOpen(), "a rejected commit must not close the batch");
    }

    @Test
    public void testCommitNotOpen() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(true, subject);

        EobBatchPublisher committed = publisher();
        committed.add(subject, data("1"));
        committed.commit();
        assertThrows(BatchPublishException.class, committed::commit);

        EobBatchPublisher discarded = publisher();
        discarded.add(subject, data("1"));
        discarded.discard();
        assertThrows(BatchPublishException.class, discarded::commit);
    }

    @Test
    public void testAtomicDisabled() throws Exception {
        String subject = NUID.nextGlobalSequence();
        createStream(false, subject);

        // ackFirst(false) so the adds are fire and forget and the error surfaces at the commit
        EobBatchPublisher bp = EobBatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"));
        BatchPublishException e = assertThrows(BatchPublishException.class, bp::commit);
        assertEquals(JS_ATOMIC_PUBLISH_DISABLED, e.getApiErrorCode());
    }

    @Test
    public void testHonorsExpectations() throws Exception {
        String subject = NUID.nextGlobalSequence();
        String streamName = createStream(true, subject);

        // only the first message of a batch may carry expectations. Set a wrong one and the
        // whole batch must be rejected at commit with nothing stored.
        BatchPublishOptions opts = BatchPublishOptions.builder().expectedLastSequence(999).build();
        EobBatchPublisher bp = EobBatchPublisher.builder().connection(nc).ackFirst(false).build();
        bp.add(subject, data("1"), opts);
        bp.add(subject, data("2"));
        assertThrows(BatchPublishException.class, bp::commit);
        assertEquals(0, msgCount(streamName));
    }

    @Test
    public void testSharedBuilderSettingsApply() {
        // the settings live on the shared base builder, so they must survive the self typing
        EobBatchPublisher bp = EobBatchPublisher.builder()
            .connection(nc)
            .batchId("my-eob-batch")
            .ackFirst(false)
            .ackEvery(5)
            .messageTtlSeconds(30)
            .ackTimeout(3000)
            .build();

        assertEquals("my-eob-batch", bp.getBatchId());
        assertFalse(bp.ackFirst());
        assertEquals(5, bp.getAckEvery());
        assertEquals("30s", bp.getMessageTtl());
        assertEquals(Duration.ofSeconds(3), bp.getAckTimeout());
    }
}
