// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.support.DateTimeUtils;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;

import static io.synadia.examples.ScheduleExampleUtils.report;

public class ScheduleAtSpecificTime {
    public static final String STREAM = "scheduler";

    public static final String SCHEDULE_PREFIX = "schedule.";
    public static final String TARGET_PREFIX = "target.";

    private static final String SCHEDULES = SCHEDULE_PREFIX + ">";
    private static final String TARGETS = TARGET_PREFIX + "*";

    public static final String[] STREAM_SUBJECTS = new String[]{SCHEDULES, TARGETS};

    public static void main(String[] args) {
        try {
            Options options = new Options.Builder()
                .server("nats://localhost:4222")
                .errorListener(new ErrorListener() {})
                .build();

            try (Connection connection = Nats.connectReconnectOnConnect(options)) {
                ScheduleExampleUtils.createOrReplaceStream(connection, STREAM, STREAM_SUBJECTS);
                JetStream js = connection.jetStream();

                CountDownLatch latch = new CountDownLatch(2);
                Dispatcher d = connection.createDispatcher();

                // subscribe to the subject that receives the schedule message
                js.subscribe(SCHEDULES, d, m -> {
                    report("SCHEDULE", m);
                    m.ack();
                }, false);

                // subscribe to the target subject
                js.subscribe(TARGETS, d, m -> {
                    report("TARGET", m);
                    m.ack();
                    latch.countDown();
                }, false);

                Message m = new ScheduledMessageBuilder()
                    .publishSubject(SCHEDULE_PREFIX + "custom")
                    .targetSubject(TARGET_PREFIX + "custom")
                    .scheduleCustom("@at 1970-01-01T00:00:00Z")
                    .data("Schedule-In-Past")
                    .build();
                report("PUBLISH", m);
                js.publish(m);

                m = new ScheduledMessageBuilder()
                    .publishSubject(SCHEDULE_PREFIX + "at")
                    .targetSubject(TARGET_PREFIX + "at")
                    .scheduleAt(DateTimeUtils.gmtNow().plusSeconds(5))
                    .data("Scheduled-In-Future")
                    .build();
                report("PUBLISH", m);
                js.publish(m);

                latch.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
