// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static io.nats.client.PublishOptions.DEFAULT_TIMEOUT;
import static io.nats.client.PublishOptions.UNSET_LAST_SEQUENCE;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Validator.*;

/**
 * Everything an atomic batch publisher does apart from how the batch is ended.
 * <p>
 * The two ways to end a batch are different enough to be different types:
 * {@link BatchPublisher} sends a final real message and stores it, while
 * {@link EobBatchPublisher} ends the batch without storing anything.
 * Both stage messages the same way, which is what lives here.
 */
public abstract class AbstractBatchPublisher {
    /** The state of the batch. Private: it is an implementation detail, not part of the api. */
    private enum State {
        Open, Closed, Discarded
    }

    /** The id of this batch. */
    protected final String batchId;

    /** The connection to publish on. */
    protected final Connection conn;

    /** How long to wait for an acknowledgement. */
    protected final Duration ackTimeout;

    /** Whether the first added message is acknowledged. */
    protected final boolean ackFirst;

    /** How often an added message is acknowledged, after the first. */
    protected final int ackEvery;

    /** The publisher level message ttl, if any. */
    protected final MessageTtl messageTtl;

    /** Re-used and cleared for every publish rather than allocated per message. */
    protected final Headers headers;

    /** The batch sequence of the most recently sent message. */
    protected int lastSeq;

    /** Where the batch stands. Subclasses read it through isOpen/requireOpen and close it
     *  through markClosed, so the private State type never leaks out of this class. */
    private State state;

    /** The subject of the first message added, which is where an EOB sentinel is addressed. */
    protected String firstSubject;

    /**
     * Construct from a builder.
     * @param b the builder
     */
    protected AbstractBatchPublisher(Builder<?, ?> b) {
        batchId = b.batchId;
        conn = b.conn;
        ackTimeout = b.ackTimeout;
        ackFirst = b.ackFirst;
        ackEvery = b.ackEvery;
        messageTtl = b.messageTtl;

        headers = new Headers();
        lastSeq = 0;
        state = State.Open;
        firstSubject = null;
    }

    /**
     * The id of this batch.
     * @return the batch id
     */
    @NonNull
    public String getBatchId() {
        return batchId;
    }

    /**
     * How long this publisher waits for an acknowledgement.
     * @return the ack timeout
     */
    @NonNull
    public Duration getAckTimeout() {
        return ackTimeout;
    }

    /**
     * Whether the first added message is acknowledged.
     * @return the flag
     */
    public boolean ackFirst() {
        return ackFirst;
    }

    /**
     * How often an added message is acknowledged, after the first. 0 means never.
     * @return the ack every value
     */
    public int getAckEvery() {
        return ackEvery;
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
     * The number of messages the batch will store.
     * @return the number of stored messages
     */
    public int size() {
        return lastSeq;
    }

    /**
     * Give up on the batch. Nothing that was staged is stored.
     */
    public void discard() {
        state = State.Discarded;
    }

    /**
     * Whether the batch is still accepting messages.
     * @return true if open
     */
    public boolean isOpen() {
        return state == State.Open;
    }

    /**
     * Whether the batch was discarded.
     * @return true if discarded
     */
    public boolean isDiscarded() {
        return state == State.Discarded;
    }

    /**
     * Whether the batch was committed.
     * @return true if closed
     */
    public boolean isClosed() {
        return state == State.Closed;
    }

    /**
     * Add a message to the batch.
     * @param subject the subject
     * @param data the payload
     * @throws BatchPublishException if the batch is not open
     */
    public void add(@NonNull String subject, byte[] data) throws BatchPublishException {
        add(subject, null, data, null);
    }

    /**
     * Add a message to the batch.
     * @param subject the subject
     * @param data the payload
     * @param opts per message options
     * @throws BatchPublishException if the batch is not open
     */
    public void add(@NonNull String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        add(subject, null, data, opts);
    }

    /**
     * Add a message to the batch.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @throws BatchPublishException if the batch is not open
     */
    public void add(@NonNull String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        add(subject, userHeaders, data, null);
    }

    /**
     * Add a message to the batch.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @param opts per message options
     * @throws BatchPublishException if the batch is not open
     */
    public void add(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        int seq = lastSeq + 1;
        if (   (seq == 1 && ackFirst)               // first publish
            || (ackEvery > 0 && seq % ackEvery == 0)) // or every publish
        {
            _addAcked(subject, userHeaders, data, opts);
        }
        else {
            _add(subject, userHeaders, data, opts);
        }
    }

    /**
     * Add a message to the batch and wait for the server to acknowledge it.
     * @param subject the subject
     * @param data the payload
     * @throws BatchPublishException if the batch is not open or the ack is invalid
     */
    public void addAcked(@NonNull String subject, byte[] data) throws BatchPublishException {
        _addAcked(subject, null, data, null);
    }

    /**
     * Add a message to the batch and wait for the server to acknowledge it.
     * @param subject the subject
     * @param data the payload
     * @param opts per message options
     * @throws BatchPublishException if the batch is not open or the ack is invalid
     */
    public void addAcked(@NonNull String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        _addAcked(subject, null, data, opts);
    }

    /**
     * Add a message to the batch and wait for the server to acknowledge it.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @throws BatchPublishException if the batch is not open or the ack is invalid
     */
    public void addAcked(@NonNull String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        _addAcked(subject, userHeaders, data, null);
    }

    /**
     * Add a message to the batch and wait for the server to acknowledge it.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @param opts per message options
     * @throws BatchPublishException if the batch is not open or the ack is invalid
     */
    public void addAcked(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        _addAcked(subject, userHeaders, data, opts);
    }

    private void _add(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        _send(subject, userHeaders, data, opts, this::publish);
    }

    private void _addAcked(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        _send(subject, userHeaders, data, opts, (s, h, d, o) -> {
            Message m = request(s, h, d, null, o);
            if (m.getData().length != 0) {
                throw ackError(m);
            }
        });
    }

    /**
     * Make the exception for a non-empty reply to a message inside the batch. The server answers
     * a message in a batch with zero bytes, so a body means it rejected the message and sent a
     * full error ack, exactly as it would on the commit. Parsing it is what keeps the server's
     * reason, "atomic publish is disabled" for instance, instead of reporting only that the
     * reply was not empty.
     * @param m the reply message
     * @return the exception to throw
     */
    private BatchPublishException ackError(Message m) {
        // never a NotSent: a reply body at all is proof the server received the message and
        // rejected it, so the batch sequence was used.
        try {
            // PublishAck's constructor is the parser the commit already relies on. An error ack
            // comes back out of it as a JetStreamApiException.
            new PublishAck(m);
        }
        catch (JetStreamApiException e) {
            return new BatchPublishException(batchId, e);
        }
        catch (IOException e) {
            // PublishAck makes an IOException when the body is not a readable ack at all, which
            // is the case the message below was written for. Fall through to it.
        }
        return new BatchPublishException(batchId, "Invalid ack returned from add with confirm");
    }

    /**
     * Thrown by a send path when the connection refused the publish before anything was queued,
     * so the batch sequence was never used and has to be given back.
     * <p>
     * Package private and never thrown out of a public method as itself: callers catch it as the
     * {@link BatchPublishException} it is. The type exists only so {@code _send} can tell "this
     * message did not leave the client" from every other failure, which it cannot do by
     * inspecting the cause, because {@link java.util.concurrent.CancellationException} is an
     * {@link IllegalStateException} and a cancellation means the message probably did leave.
     */
    static class NotSent extends BatchPublishException {
        NotSent(String batchId, Throwable cause) {
            super(batchId, cause);
        }
    }

    /** How a message is put on the wire. The only thing that differs between the two adds. */
    private interface Sender {
        void send(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException;
    }

    /**
     * The three headers this publisher writes itself. A user header of the same name is copied
     * over the top of the protocol value by {@code updateHeaders}, which corrupts the batch, so
     * they are refused rather than silently overwritten or silently dropped.
     */
    private static final Set<String> MANAGED_HEADERS = new HashSet<>(Arrays.asList(
        NATS_BATCH_ID_HDR.toLowerCase(), NATS_BATCH_SEQUENCE_HDR.toLowerCase(), NATS_BATCH_COMMIT_HDR.toLowerCase()));

    /**
     * Headers the server refuses anywhere inside a batch. {@code Nats-Expected-Last-Msg-Id} is
     * answered with 10177 for any message, including the first. {@code Nats-Msg-Id} is
     * deliberately absent: the server supports de-duplication in batches from 2.12.1 and only
     * rejects a duplicate within one batch, so refusing it outright, as the Rust client still
     * does, would block a supported feature.
     */
    private static final Set<String> UNSUPPORTED_HEADERS = new HashSet<>(Collections.singletonList(
        EXPECTED_LAST_MSG_ID_HDR.toLowerCase()));

    /**
     * Refuse user headers the batch protocol does not allow. Checked before the sequence
     * advances, so a rejected call leaves no hole.
     * @param userHeaders the caller's headers, may be null
     * @throws BatchPublishException if a header is managed by the publisher, refused by the
     *         server inside a batch, or an expected last sequence after the first message
     */
    protected void requireUserHeadersAllowed(Headers userHeaders) throws BatchPublishException {
        if (userHeaders == null || userHeaders.isEmpty()) {
            return;
        }
        for (String key : userHeaders.keySet()) {
            String lower = key.toLowerCase();
            if (MANAGED_HEADERS.contains(lower)) {
                throw new BatchPublishException(batchId,
                    "The batch publisher sets the " + key + " header itself.");
            }
            if (UNSUPPORTED_HEADERS.contains(lower)) {
                throw new BatchPublishException(batchId,
                    "The server does not allow the " + key + " header inside a batch.");
            }
            if (lastSeq > 0 && EXPECTED_LAST_SEQ_HDR.toLowerCase().equals(lower)) {
                throw new BatchPublishException(batchId,
                    "Only the first message of a batch may set an expected last sequence.");
            }
        }
    }

    /**
     * ADR-50: "Only the first message of the batch may contain {@code Nats-Expected-Last-Sequence}."
     * The server enforces that by rejecting the whole batch at commit time - 10071 when the value
     * does not match the sequence it has reached, 10164 when it does - so sending it on a later
     * message can only ever lose the batch. Checked before the sequence advances, so a rejected
     * call leaves no hole.
     * <p>
     * The restriction is narrow, and the other expectations are deliberately not included:
     * {@code Nats-Expected-Last-Subject-Sequence} is legal on any message unless an earlier
     * message in the batch wrote that same subject, which only the server can know, and
     * {@code Nats-Expected-Stream} is legal on every message.
     * @param opts the per message options, may be null
     * @throws BatchPublishException if a message after the first carries an expected last sequence
     */
    protected void requireExpectedLastSequenceOnlyOnFirst(BatchPublishOptions opts) throws BatchPublishException {
        if (lastSeq > 0 && opts != null && opts.getExpectedLastSequence() > UNSET_LAST_SEQUENCE) {
            throw new BatchPublishException(batchId,
                "Only the first message of a batch may set an expected last sequence.");
        }
    }

    /**
     * Everything an add does apart from the communication itself. The sequence advances before
     * the send because the header block carries it, and the first subject is recorded after,
     * so a non-null firstSubject means at least one message actually reached the server.
     */
    private void _send(String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts, Sender sender) throws BatchPublishException {
        requireOpen();
        requireUserHeadersAllowed(userHeaders);
        requireExpectedLastSequenceOnlyOnFirst(opts);
        ++lastSeq;
        try {
            sender.send(subject, userHeaders, data, opts);
        }
        catch (NotSent e) {
            // nothing left the client, so the sequence was never spent. Only this one failure
            // gives it back: every other, an ack error included, leaves it spent because the
            // server may already have the message.
            --lastSeq;
            throw e;
        }
        rememberFirstSubject(subject);
    }

    /**
     * Mark the batch committed, so nothing more can be added to it. Call from a commit's
     * finally block so the batch closes whether or not the server accepted it.
     */
    protected void markClosed() {
        state = State.Closed;
    }

    /**
     * Throw unless the batch is still open.
     * @throws BatchPublishException if the batch is not open
     */
    protected void requireOpen() throws BatchPublishException {
        if (state != State.Open) {
            throw new BatchPublishException(batchId, "Batch not open: " + state);
        }
    }

    /**
     * Record the subject of the first message that was actually published. Called only after a
     * successful send, so a non-null firstSubject means at least one message reached the server,
     * which is what the EOB commit's empty-batch check relies on.
     * @param subject the subject just published to
     */
    private void rememberFirstSubject(@NonNull String subject) {
        if (firstSubject == null) {
            firstSubject = subject;
        }
    }

    /**
     * Check the server's account of the batch against the client's own. ADR-50 defines
     * {@code BatchSize} as the messages the batch stored, which is what {@link #size()} counts,
     * so on an atomic batch the two must agree exactly: the batch either stored everything or
     * nothing, so there is no case where the client's count is merely an upper bound.
     * @param pa the PublishAck from the commit
     * @throws BatchPublishException if the server's account disagrees with the client's
     */
    protected void validateAck(PublishAck pa) throws BatchPublishException {
        if (pa.getBatchSize() != lastSeq) {
            throw new BatchPublishException(batchId,
                "The server reported " + pa.getBatchSize() + " messages in the batch, the client sent " + lastSeq + ".");
        }
        if (!batchId.equals(pa.getBatchId())) {
            throw new BatchPublishException(batchId,
                "The server reported batch id " + pa.getBatchId() + ".");
        }
    }

    /**
     * Build the header block and publish, without waiting for a reply. The sibling of
     * {@link #request}; there is no commitValue parameter because a commit always waits for its
     * PublishAck and so always goes through request.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @param opts per message options
     * @throws BatchPublishException if the connection rejects the publish
     */
    protected void publish(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        updateHeaders(null, userHeaders, opts);
        try {
            conn.publish(subject, headers, data);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            // jnats rejects the publish itself for an invalid subject, a closed or draining
            // connection, or a full reconnect buffer. Those are unchecked, and an add that
            // fails must fail the same way whatever rejected it. NotSent because none of them
            // reaches the outgoing queue, so nothing left the client.
            throw new NotSent(batchId, e);
        }
    }

    /**
     * Build the header block, publish, and wait for the reply.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @param commitValue null when this is not a commit, otherwise the commit header value
     * @param opts per message options
     * @return the reply message
     * @throws BatchPublishException if the request fails or times out
     */
    protected Message request(@NonNull String subject, Headers userHeaders, byte[] data, String commitValue, BatchPublishOptions opts) throws BatchPublishException {
        try {
            updateHeaders(commitValue, userHeaders, opts);
            CompletableFuture<Message> f = conn.requestWithTimeout(subject, headers, data, ackTimeout);
            return f.get(ackTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new BatchPublishException(batchId, e);
        }
        catch (CancellationException e) {
            // requestWithTimeout cancels its future when nothing answers, so this is the shape a
            // timeout actually arrives in. It is unchecked, so without this it escapes raw.
            throw new BatchPublishException(batchId, e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BatchPublishException(batchId, e);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            // Same connection level rejections as publish, and the same conclusion: nothing
            // was queued. This catch must stay below the CancellationException one, which is
            // itself an IllegalStateException and means the opposite - the message probably did
            // go out, so its sequence stays spent.
            throw new NotSent(batchId, e);
        }
    }

    /**
     * Build the header block for one message.
     * @param commitValue null when this is not a commit, otherwise {@code NATS_BATCH_COMMIT_STORE}
     *                    to commit and store the message or {@code NATS_BATCH_COMMIT_EOB} to commit
     *                    without storing it.
     * @param userHeaders headers for this message
     * @param bpOpts per message options
     */
    private void updateHeaders(String commitValue, Headers userHeaders, BatchPublishOptions bpOpts) {
        headers.clear();
        headers.put(NATS_BATCH_ID_HDR, batchId);
        // The EOB sentinel is the next message in the batch on the wire but is never stored, so
        // it takes the next sequence without lastSeq advancing. That keeps lastSeq meaning one
        // thing - the messages the batch will store - which is what size() reports and what the
        // pub ack's BatchSize counts.
        headers.put(NATS_BATCH_SEQUENCE_HDR,
            Integer.toString(NATS_BATCH_COMMIT_EOB.equals(commitValue) ? lastSeq + 1 : lastSeq));

        if (commitValue != null) {
            headers.put(NATS_BATCH_COMMIT_HDR, commitValue);
        }

        if (userHeaders != null && !userHeaders.isEmpty()) {
            Set<String> keys = userHeaders.keySet();
            for (String key : keys) {
                headers.put(key, userHeaders.get(key));
            }
        }

        if (bpOpts != null) {
            long value = bpOpts.getExpectedLastSequence();
            if (value > -1) {
                headers.put(EXPECTED_LAST_SEQ_HDR, Long.toString(value));
            }
            value = bpOpts.getExpectedLastSubjectSequence();
            if (value > -1) {
                headers.put(EXPECTED_LAST_SUB_SEQ_HDR, Long.toString(value));
            }
            String temp = bpOpts.getExpectedLastSubjectSequenceSubject();
            if (temp != null) {
                headers.put(EXPECTED_LAST_SUB_SEQ_SUB_HDR, temp);
            }
            temp = bpOpts.getExpectedStream();
            if (temp != null) {
                headers.put(EXPECTED_STREAM_HDR, temp);
            }
        }

        // The ttl is resolved outside the options block on purpose. It is the one setting that
        // exists in both places: the options value wins, and the publisher value applies to
        // every message, including the ones sent with no options at all.
        String ttl = bpOpts == null ? null : bpOpts.getMessageTtl();
        if (ttl == null) {
            ttl = messageTtl == null ? null : messageTtl.getTtlString();
        }
        if (ttl != null) {
            headers.put(MSG_TTL_HDR, ttl);
        }
    }

    /**
     * The settings both publishers share. Self typed so the setters return the concrete builder.
     * @param <B> the concrete builder type
     * @param <T> the publisher the builder makes
     */
    public abstract static class Builder<B extends Builder<B, T>, T extends AbstractBatchPublisher> {
        /**
         * Construct a builder with the default settings.
         */
        protected Builder() {}

        Connection conn;
        Duration ackTimeout;
        String batchId;
        boolean ackFirst = true;
        int ackEvery;
        MessageTtl messageTtl;

        /**
         * Return this, typed as the concrete builder.
         * @return this builder
         */
        protected abstract B self();

        /**
         * The version the server must be newer than, in the form isNewerVersionThan expects,
         * so "2.11.99" to require 2.12.0.
         * @return the version to compare against
         */
        protected abstract String newerThanVersion();

        /**
         * The message used when the server is too old.
         * @return the message
         */
        protected abstract String tooOldMessage();

        /**
         * Sets the connection. Required.
         * @param conn the connection
         * @return The Builder
         */
        public B connection(Connection conn) {
            this.conn = conn;
            return self();
        }

        /**
         * Sets the batch id. Generated when not supplied. Cannot be longer than 64 characters.
         * @param batchId the batch id
         * @return The Builder
         */
        public B batchId(String batchId) {
            this.batchId = batchId;
            return self();
        }

        /**
         * Sets the timeout, in milliseconds, to wait for an acknowledgement when adding or
         * committing. Less than 1 means use the default. Milliseconds rather than a Duration
         * because no timeout below a millisecond is reasonable, and a zero Duration is read as
         * an immediate timeout on this path and as wait forever on the fast ingest one.
         * @param ackTimeoutMillis the ack timeout in milliseconds
         * @return The Builder
         */
        public B ackTimeout(long ackTimeoutMillis) {
            this.ackTimeout = ackTimeoutMillis < 1 ? DEFAULT_TIMEOUT : Duration.ofMillis(ackTimeoutMillis);
            return self();
        }

        /**
         * Sets the timeout to wait for an acknowledgement when adding or committing.
         * <p>
         * Kept only so code built against 0.2.2 still compiles and still links. A Duration
         * invites sub-millisecond values, which are never a reasonable timeout and which jnats
         * reads as wait forever; this converts to milliseconds, so anything under a millisecond
         * becomes the default rather than an unbounded wait.
         * @param ackTimeout the ack timeout
         * @return The Builder
         * @deprecated use {@link #ackTimeout(long)} and pass milliseconds
         */
        @Deprecated
        public B ackTimeout(Duration ackTimeout) {
            return ackTimeout(ackTimeout == null ? 0 : ackTimeout.toMillis());
        }

        /**
         * Whether to ack the first message. Defaults to true
         * @param ackFirst the flag
         * @return The Builder
         */
        public B ackFirst(boolean ackFirst) {
            this.ackFirst = ackFirst;
            return self();
        }

        /**
         * The interval to ack when adding a message, after the first message. Defaults to 0 (never).
         * @param ackEvery the ack every value
         * @return The Builder
         */
        public B ackEvery(int ackEvery) {
            this.ackEvery = ackEvery < 1 ? 0 : ackEvery;
            return self();
        }

        /**
         * Sets the TTL for this specific message to be published.
         * Less than 1 has the effect of clearing the message ttl
         * @param msgTtlSeconds the ttl in seconds
         * @return The Builder
         */
        public B messageTtlSeconds(int msgTtlSeconds) {
            this.messageTtl = msgTtlSeconds < 1 ? null : MessageTtl.seconds(msgTtlSeconds);
            return self();
        }

        /**
         * Sets the TTL for this specific message to be published. Use at your own risk.
         * The current specification can be found here @see <a href="https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-43.md#per-message-ttl">JetStream Per-Message TTL</a>
         * Null or empty has the effect of clearing the message ttl
         * @param msgTtlCustom the custom ttl string
         * @return The Builder
         */
        public B messageTtlCustom(String msgTtlCustom) {
            this.messageTtl = nullOrEmpty(msgTtlCustom) ? null : MessageTtl.custom(msgTtlCustom);
            return self();
        }

        /**
         * Sets the TTL for this specific message to be published and never be expired
         * @return The Builder
         */
        public B messageTtlNever() {
            this.messageTtl = MessageTtl.never();
            return self();
        }

        /**
         * Sets the TTL for this specific message to be published
         * @param messageTtl the message ttl instance
         * @return The Builder
         */
        public B messageTtl(MessageTtl messageTtl) {
            this.messageTtl = messageTtl;
            return self();
        }

        /**
         * Validate the shared settings and fill in defaults. Call from build().
         */
        protected void validateAndDefault() {
            validateNotNull(conn, "Connection required,");
            if (!conn.getServerInfo().isNewerVersionThan(newerThanVersion())) {
                throw new IllegalArgumentException(tooOldMessage());
            }
            if (ackTimeout == null) {
                ackTimeout = conn.getOptions().getConnectionTimeout();
            }
            batchId = emptyAsNull(batchId);
            if (batchId == null) {
                batchId = new NUID().next();
            }
            else if (batchId.length() > 64) {
                throw new IllegalArgumentException("Batch ID cannot be longer than 64 characters");
            }
            else {
                // The fast publishers carry the id as one token of the reply subject, where a
                // dot would leave the server reading only the last segment as the id. The same
                // rule is applied here so one id means the same thing to both families.
                validatePrintableExceptWildDotGt(batchId, "Batch ID", true);
            }
        }

        /**
         * Build the publisher.
         * @return the publisher
         */
        public abstract T build();
    }
}
