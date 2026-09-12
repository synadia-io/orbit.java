// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.bp.BatchPublishException;
import io.synadia.bp.BatchPublishOptions;
import io.synadia.bp.BatchPublisher;

/**
 * Commit an atomic batch by sending a final real message that is stored with the rest.
 * Requires a server at 2.12.0 or later.
 */
public class BasicBatchPublishExample {
    // a main class, never instantiated
    private BasicBatchPublishExample() {}

    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "bp-stream";
    static final String SUBJECT = "bp-subject";
    static final String BATCH_ID = "bp-batch-id";
    static final int BATCH_SIZE = 1000; // !!! MAX IS 1000
    static final boolean ACK_FIRST = true; // default is true usually never change this.
    static final int AUTO_ACK_EVERY = 100; // 0 or less means no auto ack
    static final int ACK_THIS_ONE = 250;   // the one message this example acks by hand

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

            BatchPublisher publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID)
                .ackFirst(ACK_FIRST)
                .ackEvery(AUTO_ACK_EVERY)
                .build();

            // Every message you actually have is a real message,
            // but the last one is also the commit message.
            // The EobBatchPublisher does it differently
            for (int i = 1; i <= BATCH_SIZE; i++) {
                Headers h = new Headers();
                h.put("my-header", "xyz-" + i);
                byte[] data = ("data-" + i).getBytes();
                if (i == BATCH_SIZE) {
                    // commit() takes a subject and a message, and that message is stored like
                    // any other. It carries the commit rather than being extra, so the count is
                    // BATCH_SIZE and not BATCH_SIZE + 1.
                    PublishAck pa = publisher.commit(SUBJECT, h, data);
                    System.out.println("Batch [" + pa.getBatchId() + "] Committed " + pa.getJv().toJson());
                }
                else if (i == ACK_THIS_ONE) {
                    // addAcked asks the server to confirm this particular message and blocks
                    // until it does, whatever ackFirst and ackEvery are set to. Use it when one
                    // message in the batch is worth waiting on.
                    publisher.addAcked(SUBJECT, h, data);
                }
                else {
                    publisher.add(SUBJECT, h, data);
                }
            }

            // committing closes the publisher, and size() is its own count of what the batch
            // stored: BATCH_SIZE, the adds plus the commit message, which is stored like any other.
            System.out.println("Publisher size " + publisher.size()
                + ", isOpen " + publisher.isOpen() + ", isClosed " + publisher.isClosed());

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
            // The batch above left the stream at sequence 1000, but this one claims
            // expectedLastSequence(1). The server checks that under the lock at commit time,
            // sees 1000 instead of 1, and rejects the batch - so none of these messages are
            // stored, not just the one carrying the expectation.
            //
            // So the JetStreamApiException printed below ("wrong last sequence: 1000 [10071]")
            // is the expected output of a successful run, not a bug. It is caught and printed.
            publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID + "-batch-error")
                .ackFirst(false) // otherwise error will happen on first publish
                .build();
            publisher.add(SUBJECT, null, BatchPublishOptions.builder().expectedLastSequence(1).build());
            try {
                // this will exception
                publisher.commit(SUBJECT, null);
            }
            catch (BatchPublishException e) {
                System.out.println(e.getMessage());
            }

            // A batch that is started and then given up on. discard() ends it on the client
            // without committing, so nothing it added is ever stored - the stream count below
            // is unchanged. The server drops a batch it stops hearing from after 10 seconds.
            publisher = BatchPublisher.builder()
                .connection(nc)
                .batchId(BATCH_ID + "-discarded")
                .build();
            System.out.println("New publisher isOpen " + publisher.isOpen() + ", size " + publisher.size());
            publisher.add(SUBJECT, "never stored".getBytes());
            publisher.discard();
            System.out.println("After discard isOpen " + publisher.isOpen()
                + ", isDiscarded " + publisher.isDiscarded() + ", size " + publisher.size());

            si = jsm.getStreamInfo(STREAM, StreamInfoOptions.allSubjects());
            System.out.println("Stream State still shows '" + SUBJECT + "' has "
                + si.getStreamState().getSubjectMap().get(SUBJECT) + " messages.");
        }
    }
}
