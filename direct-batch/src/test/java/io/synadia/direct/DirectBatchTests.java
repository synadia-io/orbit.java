package io.synadia.direct;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.JsonUtils;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.Status.NOT_FOUND_CODE;
import static org.junit.jupiter.api.Assertions.*;

public class DirectBatchTests {
    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    @Test
    public void testBatchDirectGetErrorsAndStatuses() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                assertThrows(IllegalArgumentException.class, () -> MessageBatchGetRequest.batch(null, 1));
                assertThrows(IllegalArgumentException.class, () -> MessageBatchGetRequest.batch("", 1));
                assertThrows(IllegalArgumentException.class, () -> MessageBatchGetRequest.batch(">", 0));
                assertThrows(IllegalArgumentException.class, () -> MessageBatchGetRequest.multiLastForSubjects(null));

                assertThrows(IllegalArgumentException.class, () -> new DirectBatchContext(null, "na"));
                assertThrows(IllegalArgumentException.class, () -> new DirectBatchContext(nc, null));
                assertThrows(IllegalArgumentException.class, () -> new DirectBatchContext(nc, ""));

                JetStreamManagement jsm = nc.jetStreamManagement();
                JetStream js = nc.jetStream();

                String subject = unique();

                String streamNoDirect = unique();
                StreamConfiguration sc = StreamConfiguration.builder()
                    .name(streamNoDirect)
                    .storageType(StorageType.Memory)
                    .subjects(subject)
                    .build();
                StreamInfo si = jsm.addStream(sc);
                assertFalse(si.getConfiguration().getAllowDirect());

                // Stream doesn't have AllowDirect enabled, will error.
                assertThrows(IllegalArgumentException.class, () -> new DirectBatchContext(nc, streamNoDirect));

                String stream = unique();
                subject = unique();
                sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.Memory)
                    .subjects(subject)
                    .allowDirect(true)
                    .build();
                jsm.addStream(sc);

                DirectBatchContext db = new DirectBatchContext(nc, stream);
                MessageBatchGetRequest request = MessageBatchGetRequest.batch(subject, 3);

                // no messages yet - handler
                List<MessageInfo> list = new ArrayList<>();
                db.requestMessageBatch(request, list::add);
                verifyError(list, NOT_FOUND_CODE);

                // no messages yet - fetch
                verifyError(db.fetchMessageBatch(request), NOT_FOUND_CODE);

                // no messages yet - queue
                LinkedBlockingQueue<MessageInfo> queue = db.queueMessageBatch(request);
                verifyError(queueToList(queue), NOT_FOUND_CODE);

                js.publish(subject, unique().getBytes());

                // subject not found
                request = MessageBatchGetRequest.batch("invalid", 3);
                verifyError(db.fetchMessageBatch( request), NOT_FOUND_CODE);

                request = MessageBatchGetRequest.multiLastForSubjects(Collections.singletonList("invalid"), 3);
                verifyError(db.fetchMessageBatch(request), NOT_FOUND_CODE);

                // sequence larger
                request = MessageBatchGetRequest.batch(subject, 3, 2);
                verifyError(db.fetchMessageBatch(request), NOT_FOUND_CODE);

                List<String> subjects = Collections.singletonList(subject);

                // batch, time after
                // awaiting https://github.com/nats-io/nats-server/issues/6032
//            ZonedDateTime time = ZonedDateTime.now().plusSeconds(10);
//            request = MessageBatchGetRequest.batch(subject, 3, time);
//            verifyError(jsm.fetchMessageBatch(stream, request), NOT_FOUND_CODE);

                // last for, time before
                // awaiting https://github.com/nats-io/nats-server/issues/6077
//            time = ZonedDateTime.now().minusSeconds(10);
//            request = MessageBatchGetRequest.multiLastForSubjects(subjects, time);
//            verifyError(jsm.fetchMessageBatch(stream, request), NOT_FOUND_CODE);
            }
        }
    }

    private String unique() {
        return io.nats.client.NUID.nextGlobalSequence();
    }

    private static void verifyError(List<MessageInfo> list, int code) {
        assertEquals(1, list.size());
        MessageInfo mi = list.get(0);
        assertFalse(mi.isMessage());
        assertTrue(mi.isStatus());
        assertFalse(mi.isEobStatus());
        assertTrue(mi.isErrorStatus());
        assertEquals(code, mi.getStatus().getCode());
    }

    @Test
    public void testBatchDirectGet() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                JetStream js = nc.jetStream();
                JetStreamManagement jsm = nc.jetStreamManagement();

                String stream = unique();
                String subject = unique();
                StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.Memory)
                    .subjects(subject + ".>")
                    .allowDirect(true)
                    .build();
                StreamInfo si = jsm.addStream(sc);
                assertTrue(si.getConfiguration().getAllowDirect());

                byte[] payload = new byte[1000];
                for (int per = 1; per <= 5; per++) {
                    for (char c = 'A'; c <= 'E'; c++) {
                        String s = subject + "." + c;
                        js.publish(s, payload);
                        Thread.sleep(10); // make sure there are no duplicate times
                    }
                    Thread.sleep(2500);
                }
                ZonedDateTime time = jsm.getMessage(stream, 6).getTime().minusSeconds(1);

                String subjectAll = subject + ".>";
                String subjectA = subject + ".A";
                String subjectC = subject + ".C";
                String subjectE = subject + ".E";

                // 1/1A batch only
                // 2/2a batch with starting sequence
                // 3/3a batch with start time
                // 4/4a batch with max bytes
                // 5/5a batch with max bytes and starting sequence
                // 6/6a batch with max bytes and start time
                MessageBatchGetRequest requestBatch1 = MessageBatchGetRequest.batch(subjectAll, 3);
                MessageBatchGetRequest requestBatch1A = MessageBatchGetRequest.batch(subjectA, 3);
                MessageBatchGetRequest requestBatch2 = MessageBatchGetRequest.batch(subjectAll, 3, 4);
                MessageBatchGetRequest requestBatch2A = MessageBatchGetRequest.batch(subjectA, 3, 4);
                MessageBatchGetRequest requestBatch3 = MessageBatchGetRequest.batch(subjectAll, 3, time);
                MessageBatchGetRequest requestBatch3A = MessageBatchGetRequest.batch(subjectA, 3, time);
                MessageBatchGetRequest requestBatch4 = MessageBatchGetRequest.batchBytes(subjectAll, 3, 2002);
                MessageBatchGetRequest requestBatch4A = MessageBatchGetRequest.batchBytes(subjectA, 3, 2002);
                MessageBatchGetRequest requestBatch5 = MessageBatchGetRequest.batchBytes(subjectAll, 3, 2002, 4);
                MessageBatchGetRequest requestBatch5A = MessageBatchGetRequest.batchBytes(subjectA, 3, 2002, 4);
                MessageBatchGetRequest requestBatch6 = MessageBatchGetRequest.batchBytes(subjectAll, 3, 2002, time);
                MessageBatchGetRequest requestBatch6A = MessageBatchGetRequest.batchBytes(subjectA, 3, 2002, time);

                // 1/1A just subjects
                // 2/2A subjects with up_to_seq
                // 3/3A subjects with up_to_time
                List<String> subjectAllList = Collections.singletonList(subjectAll);
                List<String> subjectsList = Arrays.asList(subjectA, subjectC, subjectE);
                MessageBatchGetRequest requestMulti1 = MessageBatchGetRequest.multiLastForSubjects(subjectsList);
                MessageBatchGetRequest requestMulti1A = MessageBatchGetRequest.multiLastForSubjects(subjectAllList);
                MessageBatchGetRequest requestMulti2 = MessageBatchGetRequest.multiLastForSubjects(subjectsList, 22);
                MessageBatchGetRequest requestMulti2A = MessageBatchGetRequest.multiLastForSubjects(subjectAllList, 22);
                MessageBatchGetRequest requestMulti3 = MessageBatchGetRequest.multiLastForSubjects(subjectsList, time);
                MessageBatchGetRequest requestMulti3A = MessageBatchGetRequest.multiLastForSubjects(subjectAllList, time);
                MessageBatchGetRequest requestMulti4 = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectsList, 2);
                MessageBatchGetRequest requestMulti4A = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectAllList, 2);
                MessageBatchGetRequest requestMulti5 = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectsList, 22, 2);
                MessageBatchGetRequest requestMulti5A = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectAllList, 22, 2);
                MessageBatchGetRequest requestMulti6 = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectsList, time, 2);
                MessageBatchGetRequest requestMulti6A = MessageBatchGetRequest.multiLastForSubjectsBatch(subjectAllList, time, 2);

                DirectBatchContext db = new DirectBatchContext(nc, stream);

                // Get using handler.
                doHandler(db, requestBatch1, "1");
                doHandler(db, requestBatch1A, "1A");
                doHandler(db, requestBatch2, "2");
                doHandler(db, requestBatch2A, "2A");
                doHandler(db, requestBatch3, "3");
                doHandler(db, requestBatch3A, "3A");
                doHandler(db, requestBatch4, "4");
                doHandler(db, requestBatch4A, "4A");
                doHandler(db, requestBatch5, "5");
                doHandler(db, requestBatch5A, "5A");
                doHandler(db, requestBatch6, "6");
                doHandler(db, requestBatch6A, "6A");
                doHandler(db, requestMulti1, "M1");
                doHandler(db, requestMulti1A, "M1A");
                doHandler(db, requestMulti2, "M2");
                doHandler(db, requestMulti2A, "M2A");
                doHandler(db, requestMulti3, "M3");
                doHandler(db, requestMulti3A, "M3A");
                doHandler(db, requestMulti4, "M4");
                doHandler(db, requestMulti4A, "M4A");
                doHandler(db, requestMulti5, "M5");
                doHandler(db, requestMulti5A, "M5A");
                doHandler(db, requestMulti6, "M6");
                doHandler(db, requestMulti6A, "M6A");

                doFetch(db, requestBatch1, "1");
                doFetch(db, requestBatch1A, "1A");
                doFetch(db, requestBatch2, "2");
                doFetch(db, requestBatch2A, "2A");
                doFetch(db, requestBatch3, "3");
                doFetch(db, requestBatch3A, "3A");
                doFetch(db, requestBatch4, "4");
                doFetch(db, requestBatch4A, "4A");
                doFetch(db, requestBatch5, "5");
                doFetch(db, requestBatch5A, "5A");
                doFetch(db, requestBatch6, "6");
                doFetch(db, requestBatch6A, "6A");
                doFetch(db, requestMulti1, "M1");
                doFetch(db, requestMulti1A, "M1A");
                doFetch(db, requestMulti2, "M2");
                doFetch(db, requestMulti2A, "M2A");
                doFetch(db, requestMulti3, "M3");
                doFetch(db, requestMulti3A, "M3A");
                doFetch(db, requestMulti4, "M4");
                doFetch(db, requestMulti4A, "M4A");
                doFetch(db, requestMulti5, "M5");
                doFetch(db, requestMulti5A, "M5A");
                doFetch(db, requestMulti6, "M6");
                doFetch(db, requestMulti6A, "M6A");

                // Get using queue.
                doQueue(db, requestBatch1, "1");
                doQueue(db, requestBatch1A, "1A");
                doQueue(db, requestBatch2, "2");
                doQueue(db, requestBatch2A, "2A");
                doQueue(db, requestBatch3, "3");
                doQueue(db, requestBatch3A, "3A");
                doQueue(db, requestBatch4, "4");
                doQueue(db, requestBatch4A, "4A");
                doQueue(db, requestBatch5, "5");
                doQueue(db, requestBatch5A, "5A");
                doQueue(db, requestBatch6, "6");
                doQueue(db, requestBatch6A, "6A");
                doQueue(db, requestMulti1, "M1");
                doQueue(db, requestMulti1A, "M1A");
                doQueue(db, requestMulti2, "M2");
                doQueue(db, requestMulti2A, "M2A");
                doQueue(db, requestMulti3, "M3");
                doQueue(db, requestMulti3A, "M3A");
                doQueue(db, requestMulti4, "M4");
                doQueue(db, requestMulti4A, "M4A");
                doQueue(db, requestMulti5, "M5");
                doQueue(db, requestMulti5A, "M5A");
                doQueue(db, requestMulti6, "M6");
                doQueue(db, requestMulti6A, "M6A");
            }
        }
    }

    private static String miString(MessageInfo mi) {
        StringBuilder sb = JsonUtils.beginJson();
        if (mi.isStatus()) {
            JsonUtils.addField(sb, "status_code", mi.getStatus().getCode());
            JsonUtils.addField(sb, "status_message", mi.getStatus().getMessage());
        }
        else if (mi.hasError()) {
            JsonUtils.addField(sb, ERROR, mi.getError());
        }
        else {
            JsonUtils.addField(sb, SEQ, mi.getSeq());
            JsonUtils.addField(sb, LAST_SEQ, mi.getLastSeq());
            JsonUtils.addFieldWhenGteMinusOne(sb, NUM_PENDING, mi.getNumPending());
            JsonUtils.addField(sb, SUBJECT, mi.getSubject());
            JsonUtils.addField(sb, TIME, mi.getTime());
        }
        return JsonUtils.endJson(sb).toString();
    }

//    private static void debug(List<MessageInfo> list, MessageBatchGetRequest mbgr, String label) {
//        System.out.println(label + " | " + mbgr);
//        for (MessageInfo mi : list) {
//            System.out.println(miString(mi));
//        }
//        System.out.println();
//    }

    private static void doHandler(DirectBatchContext db, MessageBatchGetRequest mbgr, String label) throws Exception {
        List<MessageInfo> list = new ArrayList<>();
        db.requestMessageBatch(mbgr, list::add);
        _verify(list, label, true);
    }

    private static void doFetch(DirectBatchContext db, MessageBatchGetRequest mbgr, String label) throws Exception {
        List<MessageInfo> list = db.fetchMessageBatch(mbgr);
        _verify(list, label, false);
    }

    private static void doQueue(DirectBatchContext db, MessageBatchGetRequest mbgr, String label) throws Exception {
        LinkedBlockingQueue<MessageInfo> queue = db.queueMessageBatch(mbgr);
        _verify(queueToList(queue), label, true);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    private static void _verify(List<MessageInfo> list, String label, boolean lastIsEob) {
        switch (label) {
            case "1"   : _verify(list, 1,  23, lastIsEob, 1, 2, 3);  break;
            case "1A"  : _verify(list, 1,  3,  lastIsEob, 1, 6, 11); break;
            case "2"   : _verify(list, 4,  20, lastIsEob, 4, 5, 6);  break;
            case "2A"  : _verify(list, 6,  2,  lastIsEob, 6, 11, 16); break;
            case "3"   : _verify(list, 6,  18, lastIsEob, 6, 7, 8);  break;
            case "3A"  : _verify(list, 6,  2,  lastIsEob, 6, 11, 16); break;
            case "4"   : _verify(list, 1,  23, lastIsEob, 1, 2);  break;
            case "4A"  : _verify(list, 1,  3,  lastIsEob, 1, 6); break;
            case "5"   : _verify(list, 4,  20, lastIsEob, 4, 5);  break;
            case "5A"  : _verify(list, 6,  2,  lastIsEob, 6, 11); break;
            case "6"   : _verify(list, 6,  18, lastIsEob, 6, 7);  break;
            case "6A"  : _verify(list, 6,  2,  lastIsEob, 6, 11); break;
            case "M1"  : _verify(list, 21, 0,  lastIsEob, 21, 23, 25);  break;
            case "M1A" : _verify(list, 21, 2,  lastIsEob, 21, 22, 23, 24, 25); break;
            case "M2"  : _verify(list, 18, 0,  lastIsEob, 18, 20, 21);  break;
            case "M2A" : _verify(list, 18, 2,  lastIsEob, 18, 19, 20, 21, 22); break;
            case "M3"  : _verify(list, 1, 0,   lastIsEob, 1, 3, 5);  break;
            case "M3A" : _verify(list, 1, 2,   lastIsEob, 1, 2, 3, 4, 5); break;
            case "M4"  : _verify(list, 21, 0,  lastIsEob, 21, 23);  break;
            case "M4A" : _verify(list, 21, 2,  lastIsEob, 21, 22); break;
            case "M5"  : _verify(list, 18, 0,  lastIsEob, 18, 20);  break;
            case "M5A" : _verify(list, 18, 2,  lastIsEob, 18, 19); break;
            case "M6"  : _verify(list, 1, 0,   lastIsEob, 1, 3);  break;
            case "M6A" : _verify(list, 1, 2,   lastIsEob, 1, 2); break;
        }
    }

    private static void _verify(List<MessageInfo> list, long lastSeq1, long pending1, boolean lastIsEob, long... expected) {
        assertEquals(lastIsEob ? expected.length + 1 : expected.length, list.size());
        for (int x = 0; x < expected.length; x++) {
            MessageInfo mi = list.get(x);
            if (x == 1) {
                assertEquals(pending1, mi.getNumPending());
                assertEquals(lastSeq1, mi.getLastSeq());
            }
            assertEquals(expected[x], mi.getSeq());
            verifyMessage(mi);
        }
        if (lastIsEob) {
            verifyEob(list);
        }
    }

    private static void verifyMessage(MessageInfo mi) {
        assertTrue(mi.isMessage());
        assertFalse(mi.isStatus());
        assertFalse(mi.isEobStatus());
        assertFalse(mi.isErrorStatus());
    }

    private static void verifyEob(List<MessageInfo> list) {
        MessageInfo mi = list.get(list.size() - 1);
        assertFalse(mi.isMessage());
        assertTrue(mi.isStatus());
        assertTrue(mi.isEobStatus());
        assertFalse(mi.isErrorStatus());
    }

    private static List<MessageInfo> queueToList(LinkedBlockingQueue<MessageInfo> queue) throws InterruptedException {
        List<MessageInfo> list = new ArrayList<>();
        while (true) {
            MessageInfo mi = queue.take();
            list.add(mi);
            if (!mi.isMessage()) {
                break;
            }
        }
        return list;
    }
}
