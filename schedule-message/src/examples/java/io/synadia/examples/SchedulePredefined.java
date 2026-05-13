// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.synadia.sm.PredefinedSchedules;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: build a schedule for every entry of {@link PredefinedSchedules}
 * ({@code @hourly}, {@code @daily}, {@code @weekly}, ...). The shortest
 * predefined interval is {@code @hourly}, so this example would not fire
 * within a reasonable test run; instead it publishes each schedule, reports
 * what got stored on the stream, then cancels it.
 */
public class SchedulePredefined {

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

    private SchedulePredefined() {}

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

                ScheduleManagement.createSchedulableStream(jsm, STREAM, StorageType.Memory, STREAM_SUBJECTS);

                for (PredefinedSchedules p : PredefinedSchedules.values()) {
                    String scheduleSubject = SCHEDULE_PREFIX + p.name().toLowerCase();
                    String targetSubject   = TARGET_PREFIX   + p.name().toLowerCase();

                    // Show what is being sent to the schedule subject.
                    Message m = new ScheduledMessageBuilder()
                        .scheduleSubject(scheduleSubject)
                        .targetSubject(targetSubject)
                        .schedule(p)
                        .data("Predefined-" + p.name())
                        .build();
                    report("SCHEDULING " + p.name(), m);

                    js.publish(m);

                    // Predefined schedules fire no more often than hourly,
                    // so cancel each one immediately to keep the stream tidy.
                    report("CANCEL " + scheduleSubject,
                        ScheduleManagement.cancelSchedule(jsm, scheduleSubject, STREAM));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
