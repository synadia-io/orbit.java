// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.synadia.bp.BatchPublisher;

public class AtomicBatchDocExample {
    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "ORDERS";
    static final String SUBJECTS = "orders.>";
    static final String SUBJECT = "orders.created";
    static final String BATCH_ID = "order-4273";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Ensure an ORDERS stream exists with atomic batch publishing enabled.
            try { jsm.deleteStream(STREAM); } catch (JetStreamApiException ignore) {}
            StreamConfiguration config = StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECTS)
                .allowAtomicPublish()
                .build();
            jsm.addStream(config);

            // NATS-DOC-START
            // One order, three line items, stored as a single atomic batch:
            // either all three messages land in the stream, or none do.
            BatchPublisher publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID)
                .build();

            publisher.add(SUBJECT, "{\"sku\":\"NATS-TEE\",\"qty\":2}".getBytes());
            publisher.add(SUBJECT, "{\"sku\":\"NATS-MUG\",\"qty\":1}".getBytes());
            PublishAck ack = publisher.commit(SUBJECT, "{\"sku\":\"NATS-CAP\",\"qty\":1}".getBytes());

            System.out.println("Committed batch [" + publisher.getBatchId() + "]"
                + " of " + ack.getBatchSize() + " line items"
                + " at stream sequence " + ack.getSeqno() + ".");
            // NATS-DOC-END
        }
    }
}
