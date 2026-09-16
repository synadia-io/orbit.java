// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.bp.BatchPublishException;
import io.synadia.bp.BatchPublishOptions;
import io.synadia.bp.EobBatchPublisher;

/**
 * Commit an atomic batch without storing a final message.
 * Requires a server at 2.14.0 or later.
 */
public class BasicEobBatchPublishExample {
    // a main class, never instantiated
    private BasicEobBatchPublishExample() {}

    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "eob-stream";
    static final String SUBJECT = "eob-subject";
    static final String BATCH_ID = "eob-batch-id";
    static final int BATCH_SIZE = 5;
    static final boolean ACK_FIRST = true; // default is true usually never change this.
    static final int AUTO_ACK_EVERY = 100; // 0 or less means no auto ack

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

            JetStream js = nc.jetStream();

            EobBatchPublisher publisher = EobBatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID)
                .ackFirst(ACK_FIRST)
                .ackEvery(AUTO_ACK_EVERY)
                .build();

            // The point of EOB: every message you actually have is a real message.
            // There is no filler message held back just to carry the commit.
            for (int i = 1; i <= BATCH_SIZE; i++) {
                Headers h = new Headers();
                h.put("my-header", "xyz-" + i);
                byte[] data = ("data-" + i).getBytes();
                publisher.add(SUBJECT, h, data);
            }

            // size() is the client's own count of what the batch will store. It is the same
            // thing the ack's BatchSize reports, so the two are directly comparable.
            int sizeBeforeCommit = publisher.size();

            // commit() takes no message and no subject. The sentinel always goes to the subject
            // of the first message added. It consumed a batch sequence but was never stored,
            // so the count is BATCH_SIZE and not BATCH_SIZE + 1, and size() does not move.
            PublishAck pa = publisher.commit();
            System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getBatchSize() + " messages."
                + " Publisher size was " + sizeBeforeCommit + " before the commit and " + publisher.size() + " after,"
                + " isClosed " + publisher.isClosed() + ".");

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

            // Everything from here on is SUPPOSED to fail. It demonstrates that the expectations
            // set in BatchPublishOptions are enforced, and that a failed expectation takes the
            // whole batch with it.
            //
            // The batch above left the stream at sequence BATCH_SIZE, but this one claims
            // expectedLastSequence(1). The server checks that under the lock at commit time,
            // sees BATCH_SIZE instead of 1, and rejects the batch - so none of these messages are
            // stored, not just the one carrying the expectation.
            //
            // So the JetStreamApiException printed below ("wrong last sequence: 5 [10071]")
            // is the expected output of a successful run, not a bug. It is caught and printed.
            publisher = EobBatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID + "-batch-error")
                .ackFirst(false) // otherwise error will happen on first publish
                .build();
            publisher.add(SUBJECT, null, BatchPublishOptions.builder().expectedLastSequence(1).build());
            try {
                // this will exception
                publisher.commit();
            }
            catch (BatchPublishException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
