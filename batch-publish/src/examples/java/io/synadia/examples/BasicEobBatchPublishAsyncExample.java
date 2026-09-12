// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.synadia.bp.BatchPublishOptions;
import io.synadia.bp.EobBatchPublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Commit an atomic batch without storing a final message, asynchronously.
 * Requires a server at 2.14.0 or later.
 */
public class BasicEobBatchPublishAsyncExample {
    // a main class, never instantiated
    private BasicEobBatchPublishAsyncExample() {}

    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "eoba-stream";
    static final String SUBJECT = "eoba-subject";
    static final String BATCH_ID = "eoba-batch-id";

    /**
     * Run the example.
     * @param args unused
     * @throws Exception if anything the example does fails
     */
    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh stream that allows atomic batch publish
            try { jsm.deleteStream(STREAM); }  catch (JetStreamApiException ignore) {}
            StreamConfiguration config = StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .allowAtomicPublish()
                .build();
            jsm.addStream(config);

            EobBatchPublisher publisher = EobBatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID)
                .build();

            publisher.add(SUBJECT, null);
            publisher.add(SUBJECT, null);
            // commitAsync() takes no message and no subject. The sentinel always goes to the
            // subject of the first message added. It consumed a batch sequence but was never
            // stored, so the batch size is 2, the messages actually added.
            int sizeBeforeCommit = publisher.size();
            CompletableFuture<PublishAck> paf = publisher.commitAsync();
            PublishAck pa = paf.get(1, TimeUnit.SECONDS);
            // size() counts what the batch stores, the same thing BatchSize counts, so the
            // sentinel is in neither and the number does not move across the commit.
            System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getBatchSize() + " messages."
                + " Publisher size was " + sizeBeforeCommit + " before the commit and " + publisher.size() + " after.");

            publisher = EobBatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID + "-batch-error")
                .ackFirst(false) // otherwise error will happen on first publish
                .build();

            // The batch above left the stream at sequence 2, so this expectation cannot be met.
            // The server checks it at commit time and rejects the whole batch.
            publisher.add(SUBJECT, null, BatchPublishOptions.builder().expectedLastSequence(1).build());
            paf = publisher.commitAsync();
            try {
                // this will exception
                paf.get(1, TimeUnit.SECONDS);
            }
            catch (ExecutionException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
