// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: schedule using cron expressions, including the
 * {@link io.synadia.sm.ScheduledMessageBuilder#scheduleCron(String, String)}
 * variant that takes an IANA time zone.
 * <p>
 * NATS schedules use a six-field cron form (second minute hour day month
 * day-of-week) per ADR-51. The expressions below fire on short intervals
 * so the example completes quickly; the time-zone-bound expression behaves
 * identically here because it does not pin a time of day, but the call shape
 * is the same one you would use for {@code "0 30 9 * * *"} ("9:30 every day
 * in New York").
 */
public class ScheduleCron {

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

    private ScheduleCron() {}

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

                CountDownLatch latch = new CountDownLatch(4);
                Dispatcher d = connection.createDispatcher();

                js.subscribe(TARGETS, d, m -> {
                    report("TARGETED via '" + TARGETS + "'", m);
                    m.ack();
                    latch.countDown();
                }, false);

                String cronSubject   = SCHEDULE_PREFIX + "cron";
                String cronTzSubject = SCHEDULE_PREFIX + "cron-tz";

                // Six-field cron: every two seconds.
                report("SCHEDULING " + cronSubject + " with cron '*/2 * * * * *'");
                new ScheduledMessageBuilder()
                    .scheduleSubject(cronSubject)
                    .targetSubject(TARGET_PREFIX + "cron")
                    .scheduleCron("*/2 * * * * *")
                    .data("Cron-Every-2s")
                    .scheduleMessage(js);

                // Same expression, evaluated in a specific IANA time zone.
                report("SCHEDULING " + cronTzSubject + " with cron '*/3 * * * * *' (America/New_York)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(cronTzSubject)
                    .targetSubject(TARGET_PREFIX + "cron-tz")
                    .scheduleCron("*/3 * * * * *", "America/New_York")
                    .data("Cron-Every-3s-NY")
                    .scheduleMessage(js);

                latch.await();

                // Cron schedules keep firing until they are removed.
                report("CANCEL " + cronSubject,   ScheduleManagement.cancelSchedule(jsm, cronSubject, STREAM));
                report("CANCEL " + cronTzSubject, ScheduleManagement.cancelSchedule(jsm, cronTzSubject, STREAM));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
