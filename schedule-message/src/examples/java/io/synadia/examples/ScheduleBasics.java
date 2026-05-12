// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.DateTimeUtils;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.synadia.examples.ScheduleUtils.report;

/**
 * Example: build and publish a few scheduled messages using
 * {@link io.synadia.sm.ScheduledMessageBuilder#scheduleMessage(io.nats.client.JetStream)}.
 */
public class ScheduleBasics {

    /** Stream name used by this example. */
    public static final String STREAM = "schedules-enabled";

    /** Prefix for all schedule subjects in this example. */
    public static final String SCHEDULE_PREFIX = "schedule.";

    /** Prefix for all target subjects in this example. */
    public static final String TARGET_PREFIX = "target.";

    private static final String SCHEDULES = SCHEDULE_PREFIX + ">";
    private static final String TARGETS = TARGET_PREFIX + "*";

    /** Subject patterns the example stream accepts. */
    public static final String[] STREAM_SUBJECTS = new String[]{SCHEDULES, TARGETS};

    private ScheduleBasics() {}

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

                CountDownLatch latch = new CountDownLatch(4);
                Dispatcher d = connection.createDispatcher();

                // subscribe to the subject that receives the schedule message
                js.subscribe(SCHEDULES, d, m -> {
                    report("SCHEDULED (received)", m);
                    m.ack();
                }, false);

                // subscribe to the target subject
                js.subscribe(TARGETS, d, m -> {
                    report("TARGETED (received)", m);
                    m.ack();
                    latch.countDown();
                }, false);

                report("SCHEDULE-NOW (publishing)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(SCHEDULE_PREFIX + "now")
                    .targetSubject(TARGET_PREFIX + "now")
                    .scheduleImmediate()
                    .data("Schedule-Now")
                    .scheduleMessage(js);

                report("SCHEDULE-AT (publishing)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(SCHEDULE_PREFIX + "at")
                    .targetSubject(TARGET_PREFIX + "at")
                    .scheduleAt(DateTimeUtils.gmtNow().plusSeconds(5))
                    .data("Scheduled-At")
                    .scheduleMessage(js);

                report("SCHEDULE-EVERY (publishing)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(SCHEDULE_PREFIX + "at")
                    .targetSubject(TARGET_PREFIX + "at")
                    .scheduleEvery(1, TimeUnit.SECONDS)
                    .data("Every Second")
                    .scheduleMessage(js);

                latch.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
