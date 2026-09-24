// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.bp.*;

import static io.synadia.bp.FastPublisher.DEFAULT_MAX_FLOW;
import static io.synadia.bp.FastPublisher.DEFAULT_MAX_OUTSTANDING_ACKS;

/**
 * A fast ingest firehose in GapMode.Ok, where a dropped message is reported but does not
 * abandon the batch. Requires a server at 2.14.0 or later.
 * <p>
 * This is not atomic. Messages are persisted as they arrive, so unlike an atomic batch there
 * is no point at which nothing has been stored yet.
 */
public class FastIngestExample {
    // a main class, never instantiated
    private FastIngestExample() {}

    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "fi-stream";
    static final String SUBJECT = "fi-subject";
    static final int COUNT = 10_000;

    /**
     * Run the example.
     * @param args unused
     * @throws Exception if anything the example does fails
     */
    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // allowBatched is what opts the stream in to fast ingest
            try { jsm.deleteStream(STREAM); }  catch (JetStreamApiException ignore) {}
            jsm.addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .allowBatched(true)
                .build());

            FastPublisher fp = FastPublisher.builder()
                .connection(nc)
                .gapMode(GapMode.Ok)
                // both of these are the defaults, named rather than written as 100 and 2 so the
                // example says where they come from. MAX_FLOW_CEILING and MAX_OUTSTANDING_ACKS
                // on the same class are the upper bounds, not values to reach for.
                .maxFlow(DEFAULT_MAX_FLOW)                        // the most messages the server may go between acks
                .maxOutstandingAcks(DEFAULT_MAX_OUTSTANDING_ACKS) // how far ahead we are willing to run
                .listener(new FastPublishListener() {
                    @Override
                    public void onFlowChange(long ackEvery) {
                        // this always fires once for the server's opening rate, which may be
                        // lower than the maxFlow asked for, then again on any later change
                        System.out.println("Flow rate is now every " + ackEvery + " messages.");
                    }

                    @Override
                    public void onGap(FastFlowGap gap) {
                        System.out.println("Gap reported, continuing because this is GapMode.Ok: " + gap);
                    }
                })
                .build();

            long start = System.currentTimeMillis();
            for (int i = 1; i <= COUNT; i++) {
                // add blocks only when flow control says we are too far ahead
                FastPubAck a = fp.add(SUBJECT, ("data-" + i).getBytes());
                if (i % 2500 == 0) {
                    System.out.println("  sent " + a.getBatchSequence() + ", server acked " + a.getAckSequence());
                }
            }

            // closeBatch ends the batch, and comes in three varieties. ADR-50 calls all of them a
            // commit. Whichever is used, the PublishAck returned is the authoritative record of
            // what the batch stored.
            //
            // 1. No message (EOB). Ends the batch without storing anything more, so the batch is
            //    exactly the messages already added. Its batch size is COUNT.
            PublishAck pa = fp.closeBatch();
            //
            // 2. A final message, stored with the rest. Its batch size is COUNT + 1.
            //    To try it, comment out the line above and comment in this one:
            // PublishAck pa = fp.closeBatch(SUBJECT, "last".getBytes());
            //
            // 3. A final message with headers, stored with the rest. Its batch size is COUNT + 1.
            //    Needs import io.nats.client.impl.Headers;
            // PublishAck pa = fp.closeBatch(SUBJECT, new Headers().put("my-header", "last"), "last".getBytes());
            //
            // Not to be confused with close(), which comes from AutoCloseable and abandons the
            // batch: nothing is sent, and the server drops the batch after its idle timeout.
            long elapsed = System.currentTimeMillis() - start;

            System.out.println("Batch [" + pa.getBatchId() + "] stored " + pa.getBatchSize() + " messages in " + elapsed + "ms.");

            // flow() is the rate the server last dictated, which is not necessarily the maxFlow
            // that was asked for. gapCount() is how many gaps were reported across the batch;
            // in GapMode.Ok a non-zero count means messages were dropped and the batch went on.
            System.out.println("Client size " + fp.size() + ", final flow every " + fp.flow()
                + " messages, " + fp.gapCount() + " gaps reported.");

            StreamInfo si = jsm.getStreamInfo(STREAM);
            System.out.println("Stream has " + si.getStreamState().getMsgCount() + " messages.");
        }
    }
}
