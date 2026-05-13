// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.impl.Headers;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduledMessageBuilder;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: the three atomic publish-and-stop calls on {@link ScheduleManagement}.
 * Each one publishes a message to the {@code targetSubject} and stops the named
 * schedule as a single atomic step.
 * <ol>
 *   <li>{@link ScheduleManagement#publishAndCancelSchedule(JetStreamManagement, String, String, byte[], Headers)}
 *       — unguarded; always publishes and returns a non-null ack</li>
 *   <li>{@link ScheduleManagement#publishAndCancelScheduleIfExists(JetStreamManagement, String, String, byte[], Headers)}
 *       — guarded; returns {@code null} when the schedule is no longer present</li>
 *   <li>{@link ScheduleManagement#publishAndCancelSchedule(JetStreamManagement, String, long, String, byte[], Headers)}
 *       — explicit-sequence overload, uses
 *       {@code Nats-Expected-Last-Subject-Sequence} so the publish only succeeds
 *       while the schedule message is still at the named sequence</li>
 * </ol>
 * The example also calls the guarded form against an already-cancelled subject
 * to show the {@code null} return.
 */
public class SchedulePublishAndCancel {

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

    private SchedulePublishAndCancel() {}

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

                CountDownLatch latch = new CountDownLatch(3);
                Dispatcher d = connection.createDispatcher();

                js.subscribe(TARGETS, d, m -> {
                    report("TARGETED via '" + TARGETS + "'", m);
                    m.ack();
                    latch.countDown();
                }, false);

                String unguardedSubject = SCHEDULE_PREFIX + "unguarded";
                String guardedSubject   = SCHEDULE_PREFIX + "guarded";
                String bySeqSubject     = SCHEDULE_PREFIX + "by-seq";

                // Plant three schedules an hour out so none fire during the example.
                new ScheduledMessageBuilder()
                    .scheduleSubject(unguardedSubject)
                    .targetSubject(TARGET_PREFIX + "unguarded")
                    .scheduleIn(Duration.ofHours(1))
                    .data("placeholder")
                    .scheduleMessage(js);

                new ScheduledMessageBuilder()
                    .scheduleSubject(guardedSubject)
                    .targetSubject(TARGET_PREFIX + "guarded")
                    .scheduleIn(Duration.ofHours(1))
                    .data("placeholder")
                    .scheduleMessage(js);

                long bySeq = new ScheduledMessageBuilder()
                    .scheduleSubject(bySeqSubject)
                    .targetSubject(TARGET_PREFIX + "by-seq")
                    .scheduleIn(Duration.ofHours(1))
                    .data("placeholder")
                    .scheduleMessage(js);

                // 1) Unguarded form: publish + stop, no existence check.
                //    Always publishes and returns a non-null PublishAck (the call
                //    throws on publish failure), so no null check is needed.
                PublishAck ack1 = ScheduleManagement.publishAndCancelSchedule(
                    jsm,
                    unguardedSubject,
                    TARGET_PREFIX + "unguarded",
                    "unguarded-payload".getBytes(),
                    null);
                report("publishAndCancelSchedule (unguarded) seq=" + ack1.getSeqno());

                // 2) Guarded form: looks up the schedule first.
                //    Returns null if the schedule is no longer present (or if the
                //    stream lookup is ambiguous), in which case nothing was published.
                PublishAck ack2 = ScheduleManagement.publishAndCancelScheduleIfExists(
                    jsm,
                    guardedSubject,
                    TARGET_PREFIX + "guarded",
                    "guarded-payload".getBytes(),
                    null);
                if (ack2 == null) {
                    report("publishAndCancelScheduleIfExists (present) - schedule not found, nothing published");
                }
                else {
                    report("publishAndCancelScheduleIfExists (present) seq=" + ack2.getSeqno());
                }

                // 3) Explicit-sequence overload: uses Nats-Expected-Last-Subject-Sequence.
                //    Return type is non-nullable PublishAck; the call throws on a
                //    precondition or publish failure, so no null check is needed.
                PublishAck ack3 = ScheduleManagement.publishAndCancelSchedule(
                    jsm,
                    bySeqSubject,
                    bySeq,
                    TARGET_PREFIX + "by-seq",
                    "by-seq-payload".getBytes(),
                    null);
                report("publishAndCancelSchedule (by sequence) seq=" + ack3.getSeqno());

                // 4) Guarded form against an already-cancelled subject -> null, no publish.
                PublishAck ack4 = ScheduleManagement.publishAndCancelScheduleIfExists(
                    jsm,
                    unguardedSubject,
                    TARGET_PREFIX + "unguarded",
                    "should-not-be-sent".getBytes(),
                    null);
                if (ack4 == null) {
                    report("publishAndCancelScheduleIfExists (gone) - null as expected, nothing published");
                }
                else {
                    report("publishAndCancelScheduleIfExists (gone) unexpectedly published seq=" + ack4.getSeqno());
                }

                latch.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
