// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.synadia.bp.BatchPublisher;

/**
 * The atomic batch snippet for the NATS documentation. Only the lines between the
 * NATS-DOC-START and NATS-DOC-END markers are pulled into the docs; everything around
 * them is the setup needed to make the file runnable.
 * Requires a server at 2.12.0 or later.
 */
public class AtomicBatchDocExample {
    // a main class, never instantiated
    private AtomicBatchDocExample() {}

    static final String NATS_URL = System.getenv("NATS_URL") != null
        ? System.getenv("NATS_URL") : "nats://localhost:4222";
    static final String STREAM = "ORDERS";
    static final String SUBJECTS = "orders.>";
    static final String SUBJECT = "orders.created";
    static final String BATCH_ID = "order-4273";

    /**
     * Run the example.
     * @param args unused
     * @throws Exception if anything the example does fails
     */
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
