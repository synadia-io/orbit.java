// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.bp.BatchPublishOptions;
import io.synadia.bp.BatchPublisher;

public class ExpectationsBatchPublishExample {
    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "expect-batch";
    static final String SUBJECT_PREFIX = "expect.";
    static final String SUBJECTS = SUBJECT_PREFIX + ">";
    static final String SUBJECT_A = SUBJECT_PREFIX + "A";
    static final String SUBJECT_B = SUBJECT_PREFIX + "B";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream(STREAM); }  catch (JetStreamApiException ignore) {}
            StreamConfiguration config = StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECTS)
                .allowAtomicPublish()
                .build();
            jsm.addStream(config);

            JetStream js = nc.jetStream();

            PublishAck paA = js.publish(SUBJECT_A, "A1".getBytes());
            System.out.println("Non-Batch Publish to '" + SUBJECT_A + "' --> " + paA);

            PublishAck paB = js.publish(SUBJECT_B, "B1".getBytes());
            System.out.println("Non-Batch Publish to '" + SUBJECT_B + "' --> " + paB);

            BatchPublisher publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId("4273")
                .build();

            BatchPublishOptions.Builder bpoBuilder = BatchPublishOptions.builder()
                .expectedLastSequence(paB.getSeqno())
                .expectedLastSubjectSequence(paA.getSeqno())
                .expectedLastSubjectSequenceSubject(SUBJECT_A);

            BatchPublishOptions bpOpts = bpoBuilder.build();
            System.out.println("Batch Add to '" + SUBJECT_A + "', 'A2'");
            publisher.add(SUBJECT_A, "A2".getBytes(), bpOpts);

            // demonstrates re-use of the builder
            bpOpts = bpoBuilder.clearExpected()
                .expectedLastSubjectSequence(paB.getSeqno())
                .expectedLastSubjectSequenceSubject(SUBJECT_B)
                .build();
            System.out.println("Batch Add to '" + SUBJECT_B + "', 'B2'");
            publisher.add(SUBJECT_B, "B2".getBytes(), bpOpts);

            System.out.println("Batch Commit Add to '" + SUBJECT_A + "', 'A3'");
            PublishAck pa = publisher.commit(SUBJECT_A, "A3".getBytes());
            assert pa.getJv() != null;
            System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getJv().toJson());

            StreamInfo si = jsm.getStreamInfo(STREAM, StreamInfoOptions.allSubjects());
            System.out.println("Stream State");
            for (Subject subject : si.getStreamState().getSubjects()) {
                System.out.println("  '" + subject.getName() + "' has " + subject.getCount() + " messages.");
            }

            // simple subscription
            JetStreamSubscription sub = js.subscribe(SUBJECTS, PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .filterSubject(SUBJECTS)
                    .ackPolicy(AckPolicy.None)
                    .build())
                .build());
            System.out.println("Messages:");
            Message m = sub.nextMessage(500);
            while (m != null) {
                System.out.println(toString(m));
                m = sub.nextMessage(50);
            }
        }
    }

    public static String toString(Message msg) {
        StringBuilder sb = new StringBuilder("  '").append(msg.getSubject());
        sb.append("', '").append(new String(msg.getData())).append("'");
        Headers h = msg.getHeaders();
        if (h != null && !h.isEmpty()) {
            sb.append(", Headers:");
            for (String key : h.keySet()) {
                sb.append(System.lineSeparator()).append("      ");
                sb.append(key).append("=").append(h.get(key));
            }
        }
        return sb.toString();
    }
}
