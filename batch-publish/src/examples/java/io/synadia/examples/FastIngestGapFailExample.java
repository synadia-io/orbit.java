// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.*;
import io.synadia.bp.*;

/**
 * Fast ingest in GapMode.Fail, the ObjectStore shaped case where a gap is a hole in a file and
 * so must abandon the batch. Requires a server at 2.14.0 or later.
 * <p>
 * In this mode a gap or a per message header check failure stops the batch. The final PublishAck
 * is still authoritative about what was persisted, which is how you learn where to resume.
 */
public class FastIngestGapFailExample {
    // a main class, never instantiated
    private FastIngestGapFailExample() {}

    static final String NATS_URL = "nats://localhost:4222";
    static final String STREAM = "fi-fail-stream";
    static final String SUBJECT = "fi-fail-subject";
    static final int COUNT = 1000;

    // a field rather than a local so the listener below can query the publisher it belongs to
    static FastPublisher fp;

    /**
     * Run the example.
     * @param args unused
     * @throws Exception if anything the example does fails
     */
    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            try { jsm.deleteStream(STREAM); }  catch (JetStreamApiException ignore) {}
            jsm.addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .allowBatched(true)
                .build());

            fp = FastPublisher.builder()
                .connection(nc)
                .gapMode(GapMode.Fail)   // this is the default, shown here for clarity
                .listener(new FastPublishListener() {
                    @Override
                    public void onGap(FastFlowGap gap) {
                        // in Fail mode the batch is over. The PublishAck is still coming and
                        // reports how far the server actually got.
                        //
                        // gapCount() and getLastGap() are recorded before this callback runs, so
                        // a listener can ask the publisher rather than only reading the gap it
                        // was handed. getLastGap() here is the same object as gap.
                        System.out.println("Gap " + fp.gapCount() + " - batch abandoned: " + gap
                            + ", publisher reports " + fp.getLastGap() + ", mode " + fp.getGapMode());
                    }

                    @Override
                    public void onError(FastFlowError error) {
                        System.out.println("Message " + error.getSequence() + " failed: " + error.getDescription());
                    }
                })
                .build();

            try {
                for (int i = 1; i <= COUNT; i++) {
                    fp.add(SUBJECT, ("data-" + i).getBytes());
                    if (fp.isTerminal()) {
                        System.out.println("Batch stopped early at " + fp.size());
                        break;
                    }
                }
                PublishAck pa = fp.closeBatch();
                System.out.println("Batch [" + pa.getBatchId() + "] stored " + pa.getBatchSize() + " messages.");
            }
            catch (FastPublishException e) {
                // whatever was persisted before the failure stays persisted
                System.out.println(e.getMessage());
                System.out.println("Acked through sequence " + fp.ackedSequence());
                fp.abandon();
            }

            StreamInfo si = jsm.getStreamInfo(STREAM);
            System.out.println("Stream has " + si.getStreamState().getMsgCount() + " messages.");
        }
    }
}
