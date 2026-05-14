// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: the remaining {@code scheduleEvery(...)} and {@code scheduleIn(...)}
 * overloads on {@link ScheduledMessageBuilder} that the {@link ScheduleBasics}
 * example does not exercise:
 * <ul>
 *   <li>{@link ScheduledMessageBuilder#scheduleEvery(String)} with a Go
 *       {@code time.ParseDuration} string ({@code "2s"}, {@code "1m30s"})</li>
 *   <li>{@link ScheduledMessageBuilder#scheduleEvery(Duration)}</li>
 *   <li>{@link ScheduledMessageBuilder#scheduleIn(Duration)} (one-shot)</li>
 *   <li>{@link ScheduledMessageBuilder#scheduleIn(long, TimeUnit)} (one-shot)</li>
 * </ul>
 */
public class ScheduleEveryAndIn {

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

    private ScheduleEveryAndIn() {}

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

                CountDownLatch latch = new CountDownLatch(6);
                Dispatcher d = connection.createDispatcher();

                js.subscribe(TARGETS, d, m -> {
                    report("TARGETED via '" + TARGETS + "'", m);
                    m.ack();
                    latch.countDown();
                }, false);

                // @every using Go's time.ParseDuration syntax.
                String everyStringSubject = SCHEDULE_PREFIX + "every-string";
                report("SCHEDULING " + everyStringSubject + " with scheduleEvery(\"2s\")");
                new ScheduledMessageBuilder()
                    .scheduleSubject(everyStringSubject)
                    .targetSubject(TARGET_PREFIX + "every-string")
                    .scheduleEvery("2s")
                    .data("Every-2s-String")
                    .scheduleMessage(js);

                // @every using a java.time.Duration.
                String everyDurationSubject = SCHEDULE_PREFIX + "every-duration";
                report("SCHEDULING " + everyDurationSubject + " with scheduleEvery(Duration.ofSeconds(3))");
                new ScheduledMessageBuilder()
                    .scheduleSubject(everyDurationSubject)
                    .targetSubject(TARGET_PREFIX + "every-duration")
                    .scheduleEvery(Duration.ofSeconds(3))
                    .data("Every-3s-Duration")
                    .scheduleMessage(js);

                // One-shot via Duration.
                String inDurationSubject = SCHEDULE_PREFIX + "in-duration";
                report("SCHEDULING " + inDurationSubject + " with scheduleIn(Duration.ofSeconds(2))");
                new ScheduledMessageBuilder()
                    .scheduleSubject(inDurationSubject)
                    .targetSubject(TARGET_PREFIX + "in-duration")
                    .scheduleIn(Duration.ofSeconds(2))
                    .data("In-2s-Duration")
                    .scheduleMessage(js);

                // One-shot via (long, TimeUnit).
                String inUnitSubject = SCHEDULE_PREFIX + "in-unit";
                report("SCHEDULING " + inUnitSubject + " with scheduleIn(4, TimeUnit.SECONDS)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(inUnitSubject)
                    .targetSubject(TARGET_PREFIX + "in-unit")
                    .scheduleIn(4, TimeUnit.SECONDS)
                    .data("In-4s-Unit")
                    .scheduleMessage(js);

                latch.await();

                // The two recurring schedules will keep firing if not cancelled.
                report("CANCEL " + everyStringSubject,   ScheduleManagement.cancelSchedule(jsm, everyStringSubject,   STREAM));
                report("CANCEL " + everyDurationSubject, ScheduleManagement.cancelSchedule(jsm, everyDurationSubject, STREAM));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
