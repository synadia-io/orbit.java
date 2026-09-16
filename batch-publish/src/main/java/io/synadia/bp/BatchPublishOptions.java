// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.MessageTtl;

import static io.nats.client.PublishOptions.UNSET_LAST_SEQUENCE;
import static io.nats.client.support.Validator.*;

/**
 * Per message options, supplied when adding a message to a batch or when committing one.
 * They carry the expectations the server checks before it accepts the message, and the
 * message's ttl. A ttl set here takes precedence over the one set on the publisher.
 * <p>
 * Acknowledgement settings are deliberately not here. Whether the first message is acked, how
 * often the ones after it are, and how long to wait for an ack are all properties of the batch
 * rather than of one message, so they live on the publisher's builder. Earlier releases carried
 * copies of them here that were never read; they were removed in 0.3.0.
 */
public class BatchPublishOptions {
    /** The stream the message is expected to be stored in, or null when not set. */
    public final String expectedStream;

    /** The expected last sequence of the stream, or UNSET_LAST_SEQUENCE when not set. */
    public final long expectedLastSeq;

    /** The expected last sequence for the subject, or UNSET_LAST_SEQUENCE when not set. */
    public final long expectedLastSubSeq;

    /** The subject the expected last subject sequence applies to, which can be a wildcard, or null to use the message's own subject. */
    public final String expectedLastSubSeqSubject;

    /** The ttl for the message, or null when not set. */
    public final MessageTtl messageTtl;

    private BatchPublishOptions(Builder b) {
        this.expectedStream = b.expectedStream;
        this.expectedLastSeq = b.expectedLastSeq;
        this.expectedLastSubSeq = b.expectedLastSubSeq;
        this.expectedLastSubSeqSubject = b.expectedLastSubSeqSubject;
        this.messageTtl = b.messageTtl;
    }

    @Override
    public String toString() {
        return "BatchPublishOptions{" +
            ", expectedStream='" + expectedStream + '\'' +
            ", expectedLastSeq=" + expectedLastSeq +
            ", expectedLastSubSeq=" + expectedLastSubSeq +
            ", expectedLastSubSeqSub=" + expectedLastSubSeqSubject +
            ", messageTtl=" + getMessageTtl() +
            '}';
    }

    /**
     * Gets the expected stream.
     * @return the stream.
     */
    public String getExpectedStream() {
        return expectedStream;
    }

    /**
     * Gets the expected last sequence number of the stream.
     * @return sequence number
     */
    public long getExpectedLastSequence() {
        return expectedLastSeq;
    }

    /**
     * Gets the expected last subject sequence number of the stream.
     * @return last subject sequence number
     */
    public long getExpectedLastSubjectSequence() {
        return expectedLastSubSeq;
    }

    /**
     * Gets the expected subject to limit last subject sequence number of the stream.
     * @return the last subject sequence number's limit subject
     */
    public String getExpectedLastSubjectSequenceSubject() {
        return expectedLastSubSeqSubject;
    }

    /**
     * Gets the message ttl string. Might be null. Might be "never".
     * 10 seconds would be "10s" for the server
     * @return the message ttl string
     */
    public String getMessageTtl() {
        return messageTtl == null ? null : messageTtl.getTtlString();
    }

    /**
     * Creates a builder for the options.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * BatchPublishOptions are created using a Builder. The builder supports chaining and
     * will create a default set of options if no methods are called.
     */
    public static class Builder {
        String expectedStream;
        long expectedLastSeq = UNSET_LAST_SEQUENCE;
        long expectedLastSubSeq = UNSET_LAST_SEQUENCE;
        String expectedLastSubSeqSubject;
        MessageTtl messageTtl;

        /**
         * Constructs a new publish options Builder with the default values.
         */
        public Builder() {}

        /**
         * Sets the expected stream for the publish. If the
         * stream does not match the server will not save the message.
         * @param stream expected stream
         * @return The Builder
         */
        public Builder expectedStream(String stream) {
            expectedStream = validateStreamName(stream, false);
            return this;
        }

        /**
         * Sets the expected message sequence of the publish
         * @param sequence the expected last sequence number
         * @return The Builder
         */
        public Builder expectedLastSequence(long sequence) {
            // 0 has NO meaning to expectedLastSequence but we except 0 b/c the sequence is really a ulong
            expectedLastSeq = validateGtEqMinus1(sequence, "Last Sequence");
            return this;
        }

        /**
         * Sets the expected subject message sequence of the publish
         * @param sequence the expected last subject sequence number
         * @return The Builder
         */
        public Builder expectedLastSubjectSequence(long sequence) {
            expectedLastSubSeq = validateGtEqMinus1(sequence, "Last Subject Sequence");
            return this;
        }

        /**
         * Sets the filter subject for the expected last subject sequence
         * This can be used for a wildcard since it is used
         * in place of the message subject along with expectedLastSubjectSequence
         * @param expectedLastSubSeqSubject the filter subject
         * @return The Builder
         */
        public Builder expectedLastSubjectSequenceSubject(String expectedLastSubSeqSubject) {
            this.expectedLastSubSeqSubject = expectedLastSubSeqSubject;
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published.
         * Less than 1 has the effect of clearing the message ttl
         * @param msgTtlSeconds the ttl in seconds
         * @return The Builder
         */
        public Builder messageTtlSeconds(int msgTtlSeconds) {
            this.messageTtl = msgTtlSeconds < 1 ? null : MessageTtl.seconds(msgTtlSeconds);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published. Use at your own risk.
         * The current specification can be found here @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
         * Null or empty has the effect of clearing the message ttl
         * @param msgTtlCustom the custom ttl string
         * @return The Builder
         */
        public Builder messageTtlCustom(String msgTtlCustom) {
            this.messageTtl = nullOrEmpty(msgTtlCustom) ? null : MessageTtl.custom(msgTtlCustom);
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published and never be expired
         * @return The Builder
         */
        public Builder messageTtlNever() {
            this.messageTtl = MessageTtl.never();
            return this;
        }

        /**
         * Sets the TTL for this specific message to be published
         * @param messageTtl the message ttl instance
         * @return The Builder
         */
        public Builder messageTtl(MessageTtl messageTtl) {
            this.messageTtl = messageTtl;
            return this;
        }

        /**
         * Clears the expected so the build can be re-used.
         * Clears the following fields:
         * <ul>
         *   <li>expectedLastSequence</li>
         *   <li>expectedLastSubSeq</li>
         *   <li>expectedLastSubSeqSubject</li>
         * </ul>
         * Does not clear the following fields:
         * <ul>
         *   <li>ackTimeout</li>
         *   <li>ackFirst</li>
         *   <li>ackEvery</li>
         *   <li>expectedStream</li>
         *   <li>messageTtl</li>
         * </ul>
         * @return The Builder
         */
        public Builder clearExpected() {
            expectedLastSeq = UNSET_LAST_SEQUENCE;
            expectedLastSubSeq = UNSET_LAST_SEQUENCE;
            expectedLastSubSeqSubject = null;
            return this;
        }

        /**
         * Builds the publish options.
         * @return publish options
         */
        public BatchPublishOptions build() {
            return new BatchPublishOptions(this);
        }
    }

}
