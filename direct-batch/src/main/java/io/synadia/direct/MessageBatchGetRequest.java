package io.synadia.direct;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.Validator;

import java.time.ZonedDateTime;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for message batch get requests.
 */
public class MessageBatchGetRequest implements JsonSerializable {

    private final int batch;
    private final String nextBySubject;
    private final int maxBytes;
    private final long minSequence;
    private final ZonedDateTime startTime;
    private final List<String> multiLastBySubjects;
    private final long upToSequence;
    private final ZonedDateTime upToTime;

    // batch constructor
    private MessageBatchGetRequest(String subject,
                                   int batch,
                                   int maxBytes,
                                   long minSequence,
                                   ZonedDateTime startTime)
    {
        this.nextBySubject = Validator.required(subject, "Subject");
        this.batch = Validator.validateGtZero(batch, "Batch");
        this.maxBytes = maxBytes;
        this.startTime = startTime;
        this.multiLastBySubjects = null;
        this.upToSequence = -1;
        this.upToTime = null;

        this.minSequence = startTime == null && minSequence < 1 ? 1 : minSequence;
    }

    /**
     * Get up to batch number of messages where the message sequence is >= 1 and for the specified subject
     * @param subject the subject
     * @param batch the size of the batch
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batch(String subject, int batch) {
        return new MessageBatchGetRequest(subject, batch, -1, -1, null);
    }

    /**
     * Get up to batch number of messages where the message sequence is >= the specified sequence and for the specified subject
     * @param subject the subject
     * @param batch the size of the batch
     * @param minSequence the smallest sequence to consider
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batch(String subject, int batch, long minSequence) {
        return new MessageBatchGetRequest(subject, batch, -1, minSequence, null);
    }

    /**
     * Get up to batch number of messages where the message timestamp is >= than start time and for the specified subject
     * @param subject the subject
     * @param batch the size of the batch
     * @param startTime the earliest message timestamp to consider
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batch(String subject, int batch, ZonedDateTime startTime) {
        return new MessageBatchGetRequest(subject, batch, -1, -1, startTime);
    }

    /**
     * Get up to batch number of messages where the message sequence is >= 1, for the specified subject, and limited by max bytes
     * @param subject the subject
     * @param batch the size of the batch
     * @param maxBytes the limit of messages in bytes, determined by consumeByteCount
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batchBytes(String subject, int batch, int maxBytes) {
        return new MessageBatchGetRequest(subject, batch, maxBytes, -1, null);
    }

    /**
     * Get up to batch number of messages where the message sequence is >= than the specified sequence, for the specified subject and limited by max bytes
     * @param subject the subject
     * @param batch the size of the batch
     * @param maxBytes the limit of messages in bytes, determined by consumeByteCount
     * @param minSequence the smallest sequence to consider
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batchBytes(String subject, int batch, int maxBytes, long minSequence) {
        return new MessageBatchGetRequest(subject, batch, maxBytes, minSequence, null);
    }

    /**
     * Get up to batch number of messages where the message timestamp is >= than start time, for the specified subject and limited by max bytes
     * @param subject the subject
     * @param batch the size of the batch
     * @param maxBytes the limit of messages in bytes, determined by consumeByteCount
     * @param startTime the earliest message timestamp to consider
     * @return a MessageBatchGetRequest instance
     */
    public static MessageBatchGetRequest batchBytes(String subject, int batch, int maxBytes, ZonedDateTime startTime) {
        return new MessageBatchGetRequest(subject, batch, maxBytes, -1, startTime);
    }

    // multi for constructor
    private MessageBatchGetRequest(List<String> subjects, long upToSequence, ZonedDateTime upToTime, int batch) {
        Validator.required(subjects, "Subjects");
        this.batch = batch;
        nextBySubject = null;
        this.maxBytes = -1;
        this.minSequence = -1;
        this.startTime = null;
        this.multiLastBySubjects = subjects;
        this.upToSequence = upToSequence;
        this.upToTime = upToTime;
    }

    public static MessageBatchGetRequest multiLastForSubjects(List<String> subjects) {
        return new MessageBatchGetRequest(subjects, -1, null, -1);
    }

    public static MessageBatchGetRequest multiLastForSubjects(List<String> subjects, long upToSequence) {
        return new MessageBatchGetRequest(subjects, upToSequence, null, -1);
    }

    public static MessageBatchGetRequest multiLastForSubjects(List<String> subjects, ZonedDateTime upToTime) {
        return new MessageBatchGetRequest(subjects, -1, upToTime, -1);
    }

    public static MessageBatchGetRequest multiLastForSubjectsBatch(List<String> subjects, int batch) {
        return new MessageBatchGetRequest(subjects, -1, null, batch);
    }

    public static MessageBatchGetRequest multiLastForSubjectsBatch(List<String> subjects, long upToSequence, int batch) {
        return new MessageBatchGetRequest(subjects, upToSequence, null, batch);
    }

    public static MessageBatchGetRequest multiLastForSubjectsBatch(List<String> subjects, ZonedDateTime upToTime, int batch) {
        return new MessageBatchGetRequest(subjects, -1, upToTime, batch);
    }

    /**
     * Maximum amount of messages to be returned for this request.
     * @return batch size
     */
    public int getBatch() {
        return batch;
    }

    /**
     * Maximum amount of returned bytes for this request.
     * Limits the amount of returned messages to not exceed this.
     * @return maximum bytes
     */
    public int getMaxBytes() {
        return maxBytes;
    }

    /**
     * Minimum sequence for returned messages.
     * All returned messages will have a sequence equal to or higher than this.
     * @return minimum message sequence
     */
    public long getMinSequence() {
        return minSequence;
    }

    /**
     * Minimum start time for returned messages.
     * All returned messages will have a start time equal to or higher than this.
     * @return minimum message start time
     */
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    /**
     * Subject used to filter messages that should be returned.
     * @return the subject to filter
     */
    public String getNextBySubject() {
        return nextBySubject;
    }

    /**
     * Subjects filter used, these can include wildcards.
     * Will get the last messages matching the subjects.
     * @return the subjects to get the last messages for
     */
    public List<String> getMultiLastBySubjects() {
        return multiLastBySubjects;
    }

    /**
     * Only return messages up to this sequence.
     * @return the maximum message sequence to return results for
     */
    public long getUpToSequence() {
        return upToSequence;
    }

    /**
     * Only return messages up to this time.
     * @return the maximum message time to return results for
     */
    public ZonedDateTime getUpToTime() {
        return upToTime;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, BATCH, batch);
        addField(sb, MAX_BYTES, maxBytes);
        addField(sb, START_TIME, startTime);
        addField(sb, SEQ, minSequence);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        addStrings(sb, MULTI_LAST, multiLastBySubjects);
        addField(sb, UP_TO_SEQ, upToSequence);
        addField(sb, UP_TO_TIME, upToTime);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
