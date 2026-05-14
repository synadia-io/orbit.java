// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: the {@code Nats-Schedule-TTL} and {@code Nats-Schedule-Rollup}
 * headers, set via {@link ScheduledMessageBuilder#messageTtl(MessageTtl)} and
 * {@link ScheduledMessageBuilder#rollup()}.
 * <p>
 * <ul>
 *   <li><b>TTL</b> applies a per-message TTL to each message the schedule
 *       publishes; the stream must allow per-message TTLs (the schedulable
 *       stream helper turns this on for you).</li>
 *   <li><b>Rollup</b> sets {@code Nats-Schedule-Rollup: sub}, which causes the
 *       target subject to be rolled up on each fire — only the latest
 *       message remains on the target subject.</li>
 * </ul>
 * After a few fires this example reports the per-subject stream counts so the
 * difference between TTL'd and rolled-up targets is visible.
 */
public class ScheduleTtlAndRollup {

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

    private ScheduleTtlAndRollup() {}

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

                String ttlSubject    = SCHEDULE_PREFIX + "ttl";
                String rollupSubject = SCHEDULE_PREFIX + "rollup";
                String ttlTarget     = TARGET_PREFIX   + "ttl";
                String rollupTarget  = TARGET_PREFIX   + "rollup";

                AtomicInteger ttlHits    = new AtomicInteger();
                AtomicInteger rollupHits = new AtomicInteger();
                CountDownLatch latch = new CountDownLatch(6);
                Dispatcher d = connection.createDispatcher();

                js.subscribe(TARGETS, d, m -> {
                    if (m.getSubject().equals(ttlTarget))    ttlHits.incrementAndGet();
                    if (m.getSubject().equals(rollupTarget)) rollupHits.incrementAndGet();
                    report("TARGETED via '" + TARGETS + "'", m);
                    m.ack();
                    latch.countDown();
                }, false);

                // Each published message gets a 3-second TTL on the stream.
                report("SCHEDULING " + ttlSubject + " with messageTtl(3s)");
                new ScheduledMessageBuilder()
                    .scheduleSubject(ttlSubject)
                    .targetSubject(ttlTarget)
                    .scheduleEvery(2, TimeUnit.SECONDS)
                    .messageTtl(MessageTtl.seconds(3))
                    .data("TTL-3s")
                    .scheduleMessage(js);

                // Each fire replaces any prior message on the target subject.
                report("SCHEDULING " + rollupSubject + " with rollup()");
                new ScheduledMessageBuilder()
                    .scheduleSubject(rollupSubject)
                    .targetSubject(rollupTarget)
                    .scheduleEvery(2, TimeUnit.SECONDS)
                    .rollup()
                    .data("Rollup")
                    .scheduleMessage(js);

                latch.await();

                report("CANCEL " + ttlSubject,    ScheduleManagement.cancelSchedule(jsm, ttlSubject,    STREAM));
                report("CANCEL " + rollupSubject, ScheduleManagement.cancelSchedule(jsm, rollupSubject, STREAM));

                report("DELIVERED to " + ttlTarget,    ttlHits.get());
                report("DELIVERED to " + rollupTarget, rollupHits.get());

                // Both targets received the same number of fires, but the rollup
                // target retains only the latest message; the TTL target retains
                // each published message until it expires after 3s.
                report("STREAM count for " + ttlTarget,
                    jsm.getStreamInfo(STREAM, io.nats.client.api.StreamInfoOptions.filterSubjects(ttlTarget))
                       .getStreamState().getSubjectCount());
                report("STREAM count for " + rollupTarget,
                    jsm.getStreamInfo(STREAM, io.nats.client.api.StreamInfoOptions.filterSubjects(rollupTarget))
                       .getStreamState().getSubjectCount());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
