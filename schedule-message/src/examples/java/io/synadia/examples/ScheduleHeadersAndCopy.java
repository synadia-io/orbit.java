// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: attaching user headers to a schedule and seeding a schedule from
 * an existing {@link Message} via {@link ScheduledMessageBuilder#copy(Message)}.
 * <p>
 * The builder writes its own {@code Nats-Schedule-*} headers after copying
 * user headers, so user headers are preserved on the message that the schedule
 * publishes to the target subject.
 */
public class ScheduleHeadersAndCopy {

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

    private ScheduleHeadersAndCopy() {}

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

                CountDownLatch latch = new CountDownLatch(2);
                Dispatcher d = connection.createDispatcher();

                js.subscribe(TARGETS, d, m -> {
                    report("TARGETED via '" + TARGETS + "'", m);
                    m.ack();
                    latch.countDown();
                }, false);

                // 1) Attach custom headers to a scheduled message.
                Headers userHeaders = new Headers();
                userHeaders.put("X-Trace-Id", "abc-123");
                userHeaders.put("X-Origin",   "ScheduleHeadersAndCopy");

                String withHeadersSubject = SCHEDULE_PREFIX + "with-headers";
                report("SCHEDULING " + withHeadersSubject + " with custom headers");
                new ScheduledMessageBuilder()
                    .scheduleSubject(withHeadersSubject)
                    .targetSubject(TARGET_PREFIX + "with-headers")
                    .scheduleImmediate()
                    .headers(userHeaders)
                    .data("Has-User-Headers")
                    .scheduleMessage(js);

                // 2) Seed a schedule from an existing message via copy().
                //    copy() pulls subject, data, and headers; the schedule subject
                //    here is taken from the source message, while the target subject
                //    is set explicitly.
                Headers seedHeaders = new Headers();
                seedHeaders.put("X-Seeded-From", "template");
                Message seed = new NatsMessage(SCHEDULE_PREFIX + "copied", null, seedHeaders, "Seed-Data".getBytes());
                report("SEED message (template for copy())", seed);

                String copiedSubject = SCHEDULE_PREFIX + "copied";
                report("SCHEDULING " + copiedSubject + " built via copy(seed)");
                new ScheduledMessageBuilder()
                    .copy(seed)
                    .targetSubject(TARGET_PREFIX + "copied")
                    .scheduleImmediate()
                    .scheduleMessage(js);

                latch.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
