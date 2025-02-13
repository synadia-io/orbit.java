package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.direct.DirectBatchContext;
import io.synadia.direct.MessageBatchGetRequest;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiLastExamples {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = "ml-stream";
            String subject = "ml-sub";

            // delete the stream in case we are running this multiple times since it doesn't use a unique stream name
            try { jsm.deleteStream(stream); } catch (JetStreamApiException ignore) {}

            // Create the stream with the allow direct flag set to true
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject + ".>")
                .allowDirect(true)
                .build();
            jsm.addStream(sc);

            // publish some messages with time breaks so we can test times
            System.out.println("Publish messages.");
            byte[] payload = new byte[1000];
            for (int per = 1; per <= 5; per++) {
                if (per > 1) {
                    Thread.sleep(2500); // gives a testable time gap between groups of messages
                }
                for (char c = 'A'; c <= 'E'; c++) {
                    if (c > 'A') {
                        Thread.sleep(10); // make sure there are no duplicate times
                    }
                    String s = subject + "." + c;
                    PublishAck pa = js.publish(s, payload);
                    MessageInfo mi = jsm.getMessage(stream, pa.getSeqno());
                    ExampleUtils.printMessageInfo(mi, pa.getSeqno());
                }
            }

            // this is giving us a testable time
            ZonedDateTime time = jsm.getMessage(stream, 6).getTime().minusSeconds(1);

            String subjectAll = subject + ".>";
            String subjectA = subject + ".A";
            String subjectC = subject + ".C";
            String subjectE = subject + ".E";
            System.out.println("\nSubjects");
            System.out.println("| All: " + subjectAll);
            System.out.println("|   A: " + subjectA);
            System.out.println("|   C: " + subjectC);
            System.out.println("|   E: " + subjectE);

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

            fetchMessages(db, requestMulti1,  "M1");
            fetchMessages(db, requestMulti1A, "M1A");
            fetchMessages(db, requestMulti2,  "M2");
            fetchMessages(db, requestMulti2A, "M2A");
            fetchMessages(db, requestMulti3,  "M3");
            fetchMessages(db, requestMulti3A, "M3A");
            fetchMessages(db, requestMulti4,  "M4");
            fetchMessages(db, requestMulti4A, "M4A");
            fetchMessages(db, requestMulti5,  "M5");
            fetchMessages(db, requestMulti5A, "M5A");
            fetchMessages(db, requestMulti6,  "M6");
            fetchMessages(db, requestMulti6A, "M6A");
        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    private static void fetchMessages(DirectBatchContext db, MessageBatchGetRequest mbgr, String label) {
        List<MessageInfo> list = db.fetchMessageBatch(mbgr);
        System.out.println("\n" + label + " | " + mbgr.toJson());
        ExampleUtils.printMessageInfo(list);
    }
}
