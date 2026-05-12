package io.synadia.sm;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.List;

import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Validator.notPrintableOrHasWildGt;

/**
 * Helper utilities for stopping NATS message schedules early, per
 * <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md">ADR-51</a>
 * (section <i>Ending/stopping schedules early</i>), plus a couple of convenience helpers for
 * creating schedule-capable streams.
 * <p>
 * The class exposes two families of operations:
 * <ul>
 *   <li><b>Basic stop</b> — remove the schedule message from its stream so it can no
 *       longer fire. {@link #cancelSchedule(JetStreamManagement, String, long)} deletes by
 *       stream sequence; the subject-based overloads look the sequence up first.</li>
 *   <li><b>Atomic publish-and-stop</b> — publish a message to a different subject and stop
 *       the schedule as a single atomic step, optionally guarded by an existence check on
 *       the schedule message. See
 *       {@link #publishAndCancelSchedule(JetStreamManagement, String, String, byte[], Headers, boolean)}
 *       and {@link #publishAndCancelSchedule(JetStreamManagement, String, long, String, byte[], Headers)}.</li>
 * </ul>
 * Per the ADR the publish subject of the atomic variants must not equal the schedule
 * subject; the server rejects such publishes with error code {@code 10212}.
 * <p>
 * All methods are static; the class is {@code abstract} purely to prevent instantiation.
 */
@NullMarked
public abstract class ScheduleManagement {

    /** Utility class — not intended to be instantiated. */
    private ScheduleManagement() {}

    /**
     * Outcome of a {@code cancelSchedule(...)} call.
     */
    public enum Result {
        /** The schedule message was found and successfully deleted. */
        SUCCESS,
        /** The server-side delete returned {@code false}. */
        FAILURE,
        /** No schedule message was found for the given subject / sequence. */
        NOT_FOUND
    }

    /**
     * Add a new stream with message scheduling enabled.
     * Both {@code AllowMsgSchedules} and {@code AllowMsgTTL} are set on the stream — the
     * latter is required for the {@code Nats-Schedule-TTL} header to take effect on
     * messages produced by schedules.
     *
     * @param jsm         the JetStream management context
     * @param streamName  the stream name
     * @param storageType the storage type ({@code File} or {@code Memory})
     * @param subjects    the subjects the stream will accept; must cover both the
     *                    schedule subjects and any target subjects schedules publish to
     * @return the created {@link StreamInfo}
     * @throws JetStreamApiException if the server returned an error
     * @throws IOException           if the request could not be sent
     */
    public static StreamInfo createSchedulableStream(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects) throws JetStreamApiException, IOException {
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(storageType)
            .subjects(subjects)
            .allowMessageSchedules()
            .allowMessageTtl()
            .build();
        return jsm.addStream(sc);
    }

    /**
     * Add a new stream with message scheduling enabled, derived from an existing
     * {@link StreamConfiguration}. The supplied configuration is copied and
     * {@code AllowMsgSchedules} / {@code AllowMsgTTL} are turned on; all other settings
     * are preserved.
     *
     * @param jsm                  the JetStream management context
     * @param startingStreamConfig the base configuration to copy from
     * @return the created {@link StreamInfo}
     * @throws JetStreamApiException if the server returned an error
     * @throws IOException           if the request could not be sent
     */
    public static StreamInfo createSchedulableStream(JetStreamManagement jsm, StreamConfiguration startingStreamConfig) throws JetStreamApiException, IOException {
        StreamConfiguration sc = StreamConfiguration.builder(startingStreamConfig)
            .allowMessageSchedules()
            .allowMessageTtl()
            .build();
        return jsm.addStream(sc);
    }

    /**
     * Stop a schedule by deleting its message at a specific stream sequence (ADR-51
     * mechanism: <i>delete by stream sequence</i>). The schedule stops firing as soon as
     * its message is removed.
     *
     * @param jsm                    the JetStream management context
     * @param stream                 the stream that holds the schedule message
     * @param scheduleStreamSequence the stream sequence of the schedule message
     * @return {@link Result#SUCCESS} on a successful delete, {@link Result#FAILURE} if
     *     the server reported the delete as unsuccessful, or {@link Result#NOT_FOUND} if
     *     no message exists at that sequence (server error {@code 10043}). Any other
     *     server error is rethrown.
     * @throws JetStreamApiException if the server returned an error other than
     *     "message not found"
     * @throws IOException           if the request could not be sent
     */
    public static Result cancelSchedule(JetStreamManagement jsm, String stream, long scheduleStreamSequence) throws JetStreamApiException, IOException {
        try {
            return jsm.deleteMessage(stream, scheduleStreamSequence) ? Result.SUCCESS : Result.FAILURE;
        }
        catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == 10043) {
                return Result.NOT_FOUND;
            }
            throw e;
        }
    }

    /**
     * Convenience overload that locates the stream that owns the schedule subject before
     * delegating to {@link #cancelSchedule(JetStreamManagement, String, String)}.
     * <p>
     * The lookup is strict: it fails if zero streams match the subject, and refuses to
     * pick one when more than one stream matches. Pass the stream name explicitly to the
     * three-argument overload if you need to disambiguate.
     *
     * @param jsm             the JetStream management context
     * @param scheduleSubject the schedule subject
     * @return see {@link #cancelSchedule(JetStreamManagement, String, long)}
     * @throws IllegalStateException if no stream — or more than one — covers the subject
     * @throws JetStreamApiException if the server returned an error
     * @throws IOException           if the request could not be sent
     */
    public static Result cancelSchedule(JetStreamManagement jsm, String scheduleSubject) throws JetStreamApiException, IOException {
        return cancelSchedule(jsm, scheduleSubject, findStream(jsm, scheduleSubject));
    }

    /**
     * Stop a schedule identified by its subject in the given stream. Looks up the last
     * message on the subject, verifies it is a schedule message (has the
     * {@code Nats-Schedule} header), and deletes it by its stream sequence.
     *
     * @param jsm             the JetStream management context
     * @param scheduleSubject the exact schedule subject (wildcards are not supported)
     * @param scheduleStream  the name of the stream that holds the schedule message
     * @return {@link Result#NOT_FOUND} if no schedule message exists on the subject;
     *     otherwise the result of the underlying sequence-based delete
     * @throws JetStreamApiException if the server returned an error
     * @throws IOException           if the request could not be sent
     */
    public static Result cancelSchedule(JetStreamManagement jsm, String scheduleSubject, String scheduleStream) throws JetStreamApiException, IOException {
        if (notPrintableOrHasWildGt(scheduleSubject)) {
            // this is a wildcard subject so we must use purge
            PurgeResponse response = jsm.purgeStream(scheduleStream, PurgeOptions.builder().subject(scheduleSubject).build());
            if (response.isSuccess()) {
                return response.getPurged() > 0 ? Result.SUCCESS : Result.NOT_FOUND;
            }
            return Result.FAILURE;
        }

        long seq = getScheduleSequence(jsm, scheduleStream, scheduleSubject);
        if (seq == -1) {
            return Result.NOT_FOUND;
        }
        return cancelSchedule(jsm, scheduleStream, seq);
    }

    /**
     * Atomically publish a message and stop the named schedule, per ADR-51's
     * <i>atomic stop</i> mechanism. The published message carries:
     * <ul>
     *   <li>{@code Nats-Scheduler}: {@code scheduleSubject}</li>
     *   <li>{@code Nats-Schedule-Next}: {@code purge}</li>
     * </ul>
     * The {@code targetSubject} must not equal {@code scheduleSubject} — the server
     * rejects such publishes with error code {@code 10212}.
     *
     * @param jsm                         the JetStream management context (its
     *                                    {@code jetStream()} context is used to publish)
     * @param scheduleSubject             the schedule subject to stop
     * @param targetSubject               the subject to publish to; this may be the
     *                                    original schedule's target subject (to publish
     *                                    early) or any other subject
     * @param data                        the message body; may be {@code null}
     * @param userHeaders                 extra headers to include on the published
     *                                    message; may be {@code null}. The
     *                                    {@code Nats-Scheduler} and
     *                                    {@code Nats-Schedule-Next} headers are always
     *                                    set by this method and override any conflicting
     *                                    keys from {@code userHeaders}
     * @param publishOnlyIfScheduleExists when {@code true}, the publish is sent with an
     *                                    expected-last-subject-sequence guard so it
     *                                    only succeeds if the schedule message is still
     *                                    present; when {@code false}, the publish is
     *                                    sent unconditionally
     * @return the {@link PublishAck} from the server, or {@code null} when
     *     {@code publishOnlyIfScheduleExists} is {@code true} and no schedule for the
     *     subject could be located
     * @throws JetStreamApiException if the server returned an error
     * @throws IOException           if the request could not be sent
     */
    public static @Nullable PublishAck publishAndCancelSchedule(JetStreamManagement jsm, String scheduleSubject, String targetSubject,
                                                                byte @Nullable[] data, @Nullable Headers userHeaders, boolean publishOnlyIfScheduleExists) throws JetStreamApiException, IOException {
        if (publishOnlyIfScheduleExists) {
            String streamName = findStreamLenient(jsm, scheduleSubject);
            if (streamName != null) {
                long seq = getScheduleSequence(jsm, streamName, scheduleSubject);
                if (seq != -1) {
                    return publishAndCancelSchedule(jsm, scheduleSubject, seq, targetSubject, data, userHeaders);
                }
            }
            return null;
        }

        Headers h = makeHeaders(scheduleSubject, userHeaders);
        return jsm.jetStream().publish(targetSubject, h, data);
    }

    /**
     * Atomic publish-and-stop guarded by an explicit existence check. Same headers as
     * the simpler overload, but additionally sets:
     * <ul>
     *   <li>{@code Nats-Expected-Last-Subject-Sequence}: {@code scheduleStreamSequence}</li>
     *   <li>{@code Nats-Expected-Last-Subject-Sequence-Subject}: {@code scheduleSubject}</li>
     * </ul>
     * The publish — and therefore the stop — only succeeds if the schedule message is
     * still present at the given sequence on its subject. Useful for stopping a schedule
     * and publishing in one atomic step without risk of duplicating the message if the
     * schedule fires concurrently.
     *
     * @param jsm                    the JetStream management context
     * @param scheduleSubject        the schedule subject to stop
     * @param scheduleStreamSequence the expected stream sequence of the schedule message
     *                               on {@code scheduleSubject}
     * @param targetSubject          the subject to publish to (must differ from
     *                               {@code scheduleSubject})
     * @param data                   the message body; may be {@code null}
     * @param userHeaders            extra headers to include on the published message;
     *                               may be {@code null}. The {@code Nats-Scheduler} and
     *                               {@code Nats-Schedule-Next} headers are always set by
     *                               this method and override any conflicting keys from
     *                               {@code userHeaders}
     * @return the {@link PublishAck} from the server
     * @throws JetStreamApiException if the precondition fails or the server returned
     *     another error
     * @throws IOException           if the request could not be sent
     */
    public static PublishAck publishAndCancelSchedule(JetStreamManagement jsm, String scheduleSubject, long scheduleStreamSequence, String targetSubject,
                                                      byte @Nullable[] data, @Nullable Headers userHeaders) throws JetStreamApiException, IOException {
        Headers h = makeHeaders(scheduleSubject, userHeaders);
        PublishOptions opts = PublishOptions.builder()
            .expectedLastSubjectSequenceSubject(scheduleSubject)
            .expectedLastSubjectSequence(scheduleStreamSequence)
            .build();
        Message m = new NatsMessage(targetSubject, null, h, data);
        return jsm.jetStream().publish(m , opts);
    }

    private static Headers makeHeaders(String scheduleSubject, @Nullable Headers userHeaders) {
        Headers h = new Headers();
        h.put(NATS_SCHEDULE_NEXT_HDR, "purge");
        h.put(NATS_SCHEDULER_HDR, scheduleSubject);
        if (userHeaders != null) {
            for (String key : userHeaders.keySet()) {
                h.put(key, userHeaders.get(key));
            }
        }
        return h;
    }

    private static @Nullable String findStreamLenient(JetStreamManagement jsm, String scheduleSubject) throws JetStreamApiException, IOException {
        List<String> streams = jsm.getStreamNames(scheduleSubject);
        if (streams == null || streams.size() != 1) {
            return null;
        }
        return streams.get(0);
    }

    private static String findStream(JetStreamManagement jsm, String scheduleSubject) throws JetStreamApiException, IOException {
        List<String> streams = jsm.getStreamNames(scheduleSubject);
        if (streams == null || streams.isEmpty()) {
            throw new IllegalStateException("No stream found for subject [" + scheduleSubject + "]");
        }
        if (streams.size() != 1) {
            throw new IllegalStateException("Subject matches more than 1 stream [" + scheduleSubject + "]");
        }
        return streams.get(0);
    }

    private static long getScheduleSequence(JetStreamManagement jsm, String streamName, String scheduleSubject) throws IOException, JetStreamApiException {
        try {
            MessageInfo mi = jsm.getLastMessage(streamName, scheduleSubject);
            if (mi != null) {
                Headers headers = mi.getHeaders();
                if (headers != null && headers.containsKey(NATS_SCHEDULE_HDR)) {
                    return mi.getSeq();
                }
            }
        }
        catch (JetStreamApiException e) {
            if (e.getApiErrorCode() != 10037) {
                throw e;
            }
        }
        return -1;
    }
}
