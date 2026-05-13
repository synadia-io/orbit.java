// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static io.synadia.examples.ScheduleUtils.report;

/**
 * Example: schedule a message whose body and headers are taken from the last
 * message published on a separate source subject (the
 * {@code Nats-Schedule-Source} feature in ADR-51).
 */
public class ScheduleFromSource {

    /** Stream name used by this example. */
    public static final String STREAM = "schedules-enabled";

    private static final String SCHEDULES = "schedules";
    private static final String TARGET = "target";
    private static final String SOURCE = "source";

    /** Subject patterns the example stream accepts. */
    public static final String[] STREAM_SUBJECTS = new String[]{SCHEDULES, TARGET, SOURCE};

    private ScheduleFromSource() {}

    /**
     * Example entry point.
     * @param args ignored
     */
    public static void main(String[] args) {
        try {
            Options options = new Options.Builder()
                .server("nats://localhost:4222")
                .errorListener(new ErrorListener() {})
                .build();

            try (Connection connection = Nats.connect(options)) {
                JetStreamManagement jsm = connection.jetStreamManagement();
                JetStream js = connection.jetStream();

                // delete the stream in case it existed, just for a fresh example
                try { jsm.deleteStream(STREAM); } catch (Exception ignore) {}

                // Use the utility to properly create a schedulable stream
                StreamInfo si = ScheduleManagement.createSchedulableStream(jsm, STREAM, StorageType.Memory, STREAM_SUBJECTS);
                report("Created stream", si.getConfiguration());

                CountDownLatch latch1 = new CountDownLatch(1);
                CountDownLatch latch2 = new CountDownLatch(2);
                Dispatcher d = connection.createDispatcher();

                // subscribe to the subject that receives the schedule message
                js.subscribe(SCHEDULES, d, m -> {
                    report("SCHEDULED (received)", m);
                    m.ack();
                }, false);

                // subscribe to the target subject
                js.subscribe(SOURCE, d, m -> {
                    report("SOURCED (received)", m);
                    m.ack();
                }, false);

                // subscribe to the target subject
                js.subscribe(TARGET, d, m -> {
                    report("TARGETED (received)", m);
                    m.ack();
                    latch1.countDown();
                    latch2.countDown();
                }, false);

                // Publish Data to the Source subject
                String sourceData = "data1";
                Headers sourceHeaders = new Headers();
                sourceHeaders.put("foo1", "bar1");
                Message sourceMessage = new NatsMessage(SOURCE, null, sourceHeaders, sourceData.getBytes());
                report("SOURCE 1 (publishing)", sourceMessage);
                js.publish(sourceMessage);
                connection.flush(Duration.ofSeconds(1));

                Message scheduleMessage = new ScheduledMessageBuilder()
                    .scheduleSubject(SCHEDULES)
                    .targetSubject(TARGET)
                    .scheduleImmediate()
                    .sources(SOURCE)
                    .build();
                report("SCHEDULE 1 (publishing)", scheduleMessage);
                js.publish(scheduleMessage);

                latch1.await();

                sourceData = "data2";
                sourceHeaders = new Headers();
                sourceHeaders.put("foo2", "bar2");
                sourceMessage = new NatsMessage(SOURCE, null, sourceHeaders, sourceData.getBytes());
                report("SOURCE 2 (publishing)", sourceMessage);
                js.publish(sourceMessage);
                connection.flush(Duration.ofSeconds(1));

                report("SCHEDULE 2 (publishing)", scheduleMessage);
                js.publish(scheduleMessage);

                latch2.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
