// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.StorageType;
import io.synadia.sm.ScheduleManagement;
import io.synadia.sm.ScheduleManagement.Result;
import io.synadia.sm.ScheduledMessageBuilder;

import java.time.Duration;

import static io.synadia.examples.ScheduleExampleUtils.report;

/**
 * Example: stop a running schedule with each
 * {@link ScheduleManagement#cancelSchedule cancelSchedule} overload.
 * <p>
 * <ul>
 *   <li>by stream + sequence — the lowest-level call</li>
 *   <li>by stream + subject — looks up the sequence on a known stream</li>
 *   <li>by subject only — the helper locates the stream too</li>
 * </ul>
 * The example also shows a {@link Result#NOT_FOUND} outcome by cancelling a
 * subject that no longer has a schedule.
 */
public class ScheduleCancel {

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

    private ScheduleCancel() {}

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

                String bySeqSubject     = SCHEDULE_PREFIX + "by-seq";
                String bySubjectSubject = SCHEDULE_PREFIX + "by-subject";
                String byLookupSubject  = SCHEDULE_PREFIX + "by-lookup";

                // Schedule each one an hour out so it can't fire while the example runs.
                long bySeq = new ScheduledMessageBuilder()
                    .scheduleSubject(bySeqSubject)
                    .targetSubject(TARGET_PREFIX + "by-seq")
                    .scheduleIn(Duration.ofHours(1))
                    .data("By-Seq")
                    .scheduleMessage(js);
                report("SCHEDULED " + bySeqSubject + " at sequence " + bySeq);

                new ScheduledMessageBuilder()
                    .scheduleSubject(bySubjectSubject)
                    .targetSubject(TARGET_PREFIX + "by-subject")
                    .scheduleIn(Duration.ofHours(1))
                    .data("By-Subject")
                    .scheduleMessage(js);
                report("SCHEDULED " + bySubjectSubject);

                new ScheduledMessageBuilder()
                    .scheduleSubject(byLookupSubject)
                    .targetSubject(TARGET_PREFIX + "by-lookup")
                    .scheduleIn(Duration.ofHours(1))
                    .data("By-Lookup")
                    .scheduleMessage(js);
                report("SCHEDULED " + byLookupSubject);

                // 1) Sequence-based cancel.
                Result r1 = ScheduleManagement.cancelSchedule(jsm, STREAM, bySeq);
                report("CANCEL by sequence", r1);

                // 2) Subject + stream cancel.
                Result r2 = ScheduleManagement.cancelSchedule(jsm, bySubjectSubject, STREAM);
                report("CANCEL by subject + stream", r2);

                // 3) Subject-only cancel; the helper locates the stream.
                Result r3 = ScheduleManagement.cancelSchedule(jsm, byLookupSubject);
                report("CANCEL by subject (auto-find)", r3);

                // 4) Cancelling something that isn't there returns NOT_FOUND.
                Result r4 = ScheduleManagement.cancelSchedule(jsm, bySeqSubject, STREAM);
                report("CANCEL already-cancelled", r4);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
