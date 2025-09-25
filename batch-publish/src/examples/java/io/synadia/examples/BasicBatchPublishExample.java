// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.bp.BatchPublisher;

public class BasicBatchPublishExample {
    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "bp-stream";
    static final String SUBJECT = "bp-subject";
    static final int BATCH_SIZE = 1000; // !!! MAX IS 1000
    static final boolean ACK_FIRST = true; // default is true usually never change this.
    static final int AUTO_ACK_EVERY = 100; // 0 or less means no auto ack

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream(STREAM); }  catch (JetStreamApiException ignore) {}
            StreamConfiguration config = StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .allowAtomicPublish()
                .build();
            jsm.addStream(config);

            JetStream js = nc.jetStream();

            BatchPublisher publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(NUID.nextGlobal())
                .ackFirst(ACK_FIRST)
                .ackEvery(AUTO_ACK_EVERY)
                .build();

            for (int i = 1; i <= BATCH_SIZE; i++) {
                Headers h = new Headers();
                h.put("my-header", "xyz-" + i);
                byte[] data = ("data-" + i).getBytes();
                if (i == BATCH_SIZE) {
                    PublishAck pa = publisher.commit(SUBJECT, h, data);
                    assert pa.getJv() != null;
                    System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getJv().toJson());
                }
                else {
                    publisher.add(SUBJECT, h, data);
                }
            }

            StreamInfo si = jsm.getStreamInfo(STREAM, StreamInfoOptions.allSubjects());
            long messages = si.getStreamState().getSubjectMap().get(SUBJECT);
            System.out.println("Stream State shows '" + SUBJECT + "' has " + messages + " messages.");

            // simple subscription
            JetStreamSubscription sub = js.subscribe(SUBJECT, PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .filterSubject(SUBJECT)
                    .ackPolicy(AckPolicy.None)
                    .build())
                .build());
            int count = 0;
            Message m = sub.nextMessage(500);
            while (m != null) {
                count++;
                m = sub.nextMessage(50);
            }
            System.out.println("Consumed " + count + " messages from '" + SUBJECT + "'");
        }
    }

    public static String toString(Message msg) {
        StringBuilder sb = new StringBuilder(System.lineSeparator())
            .append("  Subject: ").append(msg.getSubject());
        if (msg.getData() == null || msg.getData().length == 0) {
            sb.append(" | No Data");
        }
        else {
            sb.append(" | Data: ").append(new String(msg.getData()));
        }
        Headers h = msg.getHeaders();
        if (h != null && !h.isEmpty()) {
            sb.append(System.lineSeparator()).append("  Headers:");
            for (String key : h.keySet()) {
                sb.append(System.lineSeparator()).append("    ");
                sb.append(key).append("=").append(h.get(key));
            }
        }
        return sb.toString();
    }
}
