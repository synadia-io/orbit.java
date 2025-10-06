// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.synadia.bp.BatchPublishOptions;
import io.synadia.bp.BatchPublisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BasicBatchPublishAsyncExample {
    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "bpa-stream";
    static final String SUBJECT = "bpa-subject";
    static final String BATCH_ID = "bpa-batch-id";

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

            BatchPublisher publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID)
                .build();

            publisher.add(SUBJECT, null);
            CompletableFuture<PublishAck> paf = publisher.commitAsync(SUBJECT, null);
            PublishAck pa = paf.get(1, TimeUnit.SECONDS);
            System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getJv());

            publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID + "-batch-error")
                .ackFirst(false) // otherwise error will happen on first publish
                .build();

            publisher.add(SUBJECT, null, BatchPublishOptions.builder().expectedLastSequence(1).build());
            paf = publisher.commitAsync(SUBJECT, null);
            try {
                // this will exception
                paf.get(1, TimeUnit.SECONDS);
            }
            catch (ExecutionException e) {
                //noinspection ThrowablePrintedToSystemOut
                System.out.println(e);
            }
        }
    }
}
