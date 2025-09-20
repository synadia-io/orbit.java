// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.synadia.bp.BatchPublisher;

public class BatchPublishExample {
    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "bp-stream";
    static final String SUBJECT = "bp-subject";
    static final int BATCH_SIZE = 1000; // !!! MAX IS 1000
    static final int CONFIRM_EVERY = 100;

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
                .build();

            for (int i = 1; i <= BATCH_SIZE; i++) {
                Headers h = new Headers();
                h.put("my-id", "xyz-" + i);
                byte[] data = ("data-" + i).getBytes();
                if (i == BATCH_SIZE) {
                    System.out.println("Commit");
                    PublishAck pa = publisher.commit(SUBJECT, h, data);
                    assert pa.getJv() != null;
                    System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getJv().toJson());
                }
                else if (CONFIRM_EVERY > 0 && i % CONFIRM_EVERY == 0) {
                    publisher.addWithConfirm(SUBJECT, h, data);
                    System.out.println("Batch [" + publisher.getBatchId() + "] Progress confirmed at message " + i);
                }
                else {
                    publisher.add(SUBJECT, h, data);
                }
            }

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
            System.out.println("Retrieved " + count + " messages.");
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
