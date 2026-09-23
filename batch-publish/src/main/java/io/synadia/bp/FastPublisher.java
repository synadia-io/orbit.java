// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.readLong;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Validator.*;

/**
 * Publishes a fast ingest batch.
 * <p>
 * A fast ingest batch has no size limit, so ending one is simply closing it, and there are two
 * ways to do that: {@link #closeBatch(String, byte[])} sends a final real message that is stored
 * along with the rest, and {@link #closeBatch()} ends the batch <b>without storing a final
 * message</b>, which ADR-50 calls EOB. ADR-50 calls both a commit, operations 2 and 3.
 * <p>
 * Requires a server at 2.14.0 or later and a stream configured with {@code allow_batched}.
 * <p>
 * <b>This is not atomic.</b> Unlike the atomic publishers there is no staging and no all or
 * nothing guarantee. Messages are persisted as they arrive, the batch has no size limit, and
 * messages can be dropped by the server's overload protection or lost across a stream leader
 * change. What you get in exchange is a control channel over which the server continuously tells
 * you how fast you are allowed to go. Choose {@link GapMode} to say whether a gap should abandon
 * the batch or merely be reported.
 * <p>
 * These publishers are <b>not thread safe</b> and are meant to be owned by one producer thread.
 * Multiple concurrent producers should each hold their own, which is exactly the scenario the
 * server's flow control is designed around.
 * <p>
 * The control channel is read on a dispatcher of this publisher's own, so the work is split in
 * two. That thread does one thing: decide whether an arriving message means the server has ended
 * the batch, and record it. Everything else - the flow accounting, the gap and error counters,
 * the terminal ack, and <b>every listener callback</b> - runs on the thread that called
 * {@code add}, {@code closeBatch} or {@code ping}, in arrival order. So a batch the server abandons
 * while the application is between publishes is known to be over immediately, through
 * {@link #isTerminal()} and {@link #getEndReason()}, while nothing a listener does can run on a
 * thread the application did not expect.
 * <p>
 * These implement {@link AutoCloseable} so try-with-resources releases the control channel,
 * which is the one resource a fast publisher owns: its dispatcher, and the thread that comes
 * with it. {@link #close()} is exactly
 * {@link #abandon()} and never commits: try-with-resources runs it on the exception path too,
 * where committing a batch whose assembly had just thrown would be precisely backwards.
 * Use {@code closeBatch} to end a batch deliberately.
 * <p>
 * The server abandons a batch that receives nothing for
 * {@link BatchUtils#getBatchIdleTimeoutSeconds(Connection) its idle timeout}, 10 seconds by
 * default, and says nothing when it does. So once the first message is out, the publisher pings
 * the batch whenever nothing has been sent for the {@link Builder#idlePingSeconds(int) idle ping}
 * interval, which keeps it alive for as long as the application takes to close it. The ping is
 * sent from a shared timer thread; its answer is processed on the publishing thread like every
 * other control message. The flip side is that a batch that is never closed with
 * {@code closeBatch}, abandoned, or closed with {@code close()} is kept alive indefinitely,
 * along with this publisher.
 * <p>
 * The publisher also pings on its own while it waits: every third of the ack timeout while flow
 * control holds it back, and while {@code closeBatch} waits for its PublishAck. That is how a lost
 * flow acknowledgement is recovered, since the server answers a ping with its current flow state.
 */
public class FastPublisher implements AutoCloseable {
    /**
     * The default upper bound on how many messages the server may go between flow
     * acknowledgements, when the user does not choose one. ADR-50 does not mandate a number;
     * this is the value its worked example uses.
     */
    public static final int DEFAULT_MAX_FLOW = 100;

    /** The largest max flow the subject grammar can carry, since the server reads it as a uint16. */
    public static final int MAX_FLOW_CEILING = 65535;

    /**
     * The default number of acknowledgements the client will let go outstanding before it
     * blocks. ADR-50 recommends 1 or 2.
     */
    public static final int DEFAULT_MAX_OUTSTANDING_ACKS = 2;

    /** The most outstanding acknowledgements ADR-50 suggests exposing. */
    public static final int MAX_OUTSTANDING_ACKS = 3;

    /**
     * One thread shared by every fast publisher in the process, for idle pings only. Daemon, so
     * it never holds the jvm open, and cancelled pings are removed rather than left in the queue.
     */
    private static final ScheduledThreadPoolExecutor IDLE_PINGER = idlePinger();

    private static ScheduledThreadPoolExecutor idlePinger() {
        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "fast-publish-idle-ping");
            t.setDaemon(true);
            return t;
        });
        stpe.setRemoveOnCancelPolicy(true);
        return stpe;
    }

    private final String batchId;
    private final Connection conn;

    private final Duration ackTimeout;
    private final GapMode gapMode;
    private final int maxOutstandingAcks;
    private final FastPublishListener listener;
    private final Dispatcher dispatcher;
    private final BlockingQueue<Message> control = new LinkedBlockingQueue<>();
    private final String replyPrefix; // everything up to the per message seq
    private final int idlePingSeconds;
    private final long idlePingNanos; // 0 means the publisher does not ping an idle batch

    private long batchSeq;        // the batch sequence of the most recently sent message
    private String firstSubject;  // the subject of the first message added, where an EOB sentinel is addressed

    private long lastAckSeq;
    private long ackEvery;       // server dictated, 0 until the server tells us
    private volatile boolean terminal;                 // written by the dispatcher thread too
    private final AtomicReference<EndReason> endReason = new AtomicReference<>(EndReason.Open);
    private boolean abandoned;
    private PublishAck finalAck; // the batch's terminal PublishAck, kept once it is seen
    private boolean terminalAckReceived; // the terminal PublishAck arrived, whether or not it was an error
    private FastFlowError pendingError;
    private long gapCount;       // proposal, see gapCount()
    private FastFlowGap lastGap; // proposal, see getLastGap()

    // Shared with the idle ping thread. sentSeq is the highest batch sequence already handed to
    // the connection, which is the only sequence a ping from another thread may carry: batchSeq
    // advances before its message is published, and a ping carrying it could reach the server
    // first and be reported as a gap, which ends a GapMode.Fail batch.
    private volatile long sentSeq;
    private volatile long lastSendNanos;
    private final Object idlePingLock = new Object();
    private boolean idlePingStopped;          // guarded by idlePingLock
    private ScheduledFuture<?> idlePingTask;  // guarded by idlePingLock

    private FastPublisher(Builder b) {
        batchId = b.batchId;
        conn = b.conn;
        ackTimeout = b.ackTimeout;
        gapMode = b.gapMode;
        maxOutstandingAcks = b.maxOutstandingAcks;
        listener = b.listener;
        idlePingSeconds = b.idlePingSeconds;
        idlePingNanos = TimeUnit.SECONDS.toNanos(idlePingSeconds);

        // A fresh inbox, never the request/mux inbox. ADR-50 calls this out: on an error the
        // server may have thousands of queued acks to send, and dropping interest entirely lets
        // it short circuit them.
        //
        // The subscription is owned by a dispatcher of this publisher's own rather than read
        // synchronously, so the control channel is processed the moment something arrives even
        // if the application has gone quiet. A dedicated dispatcher rather than a shared one:
        // it is a thread of its own, so classifying this batch's control messages never contends
        // with anything else on the connection, and closing it releases that thread.
        String prefix = conn.createInbox();
        dispatcher = conn.createDispatcher();
        dispatcher.subscribe(prefix + "." + batchId + ".>", this::onControlMessage);

        // Cached because only seq and op change per message, and this is the hot path.
        // maxFlow is stated to the server once, here, and never needed again.
        replyPrefix = prefix + "." + batchId + "." + b.maxFlow + "." + gapMode + ".";

        batchSeq = 0;
        lastAckSeq = 0;
        ackEvery = 0;
        gapCount = 0;
        terminal = false;
        abandoned = false;
        firstSubject = null;
    }

    // ------------------------------------------------------------------------------------
    // accessors
    // ------------------------------------------------------------------------------------

    /**
     * The id of this batch.
     * @return the batch id
     */
    @NonNull
    public String getBatchId() {
        return batchId;
    }

    /**
     * The gap mode this batch was started with.
     * @return the gap mode
     */
    @NonNull
    public GapMode getGapMode() {
        return gapMode;
    }

    /**
     * The number of messages the batch will store.
     * @return the number of stored messages
     */
    public long size() {
        return batchSeq;
    }

    /**
     * The highest batch sequence the server has acknowledged. Acks are cumulative.
     * @return the acknowledged batch sequence
     */
    public long ackedSequence() {
        return lastAckSeq;
    }

    /**
     * The current server dictated flow rate, meaning the server acknowledges every this many
     * messages. Zero until the server answers the first message.
     * @return the flow rate
     */
    public long flow() {
        return ackEvery;
    }

    /**
     * Whether this batch is finished, either committed or stopped by a gap or error in
     * {@link GapMode#Fail}.
     * @return true if the batch can no longer accept messages
     */
    public boolean isTerminal() {
        return terminal || abandoned;
    }

    /**
     * Why the batch ended, or {@link EndReason#Open} while it is still running.
     * <p>
     * {@link #isTerminal()} answers whether the batch is over; this answers what ended it, which
     * a caller needs to tell a committed batch from one the server abandoned under it. The
     * guarantee between them is one directional and is the direction callers use: once
     * {@code isTerminal()} is true this is never {@code Open}. It can be set an instant before
     * the batch reads as terminal, which is harmless.
     * @return the reason
     */
    @NonNull
    public EndReason getEndReason() {
        return endReason.get();
    }

    /**
     * How long the batch may go without a message before this publisher pings it, in seconds.
     * @return the idle ping interval in seconds, or 0 when the publisher does not ping an idle batch
     */
    public int getIdlePingSeconds() {
        return idlePingSeconds;
    }

    /**
     * The last error the server reported for a message in this batch, if any.
     * @return the error or null
     */
    @Nullable
    public FastFlowError getPendingError() {
        return pendingError;
    }

    // ------------------------------------------------------------------------------------
    // gap insight. Proposals, not part of ADR-50.
    // ------------------------------------------------------------------------------------

    /**
     * How many gaps the server has reported for this batch.
     * <p>
     * <b>Proposal.</b> ADR-50 defines the gap report but says nothing about a client counting
     * them. The C, Go, .NET, Python and Rust clients each hand a gap to a callback and keep
     * nothing but a fatal flag, so none of them can answer this after the fact. In
     * {@link GapMode#Fail} the count is 0 or 1, since the first gap ends the batch. In
     * {@link GapMode#Ok} it is the number of times the batch was interrupted, which is what
     * the user of an Ok mode batch is actually asking when they ask how the batch went.
     * <p>
     * This counts reports, not lost messages. ADR-50 states that a gap report MUST NOT be used
     * to determine what was persisted, so no count of lost messages is derived from it here.
     * Only the final {@link io.nats.client.api.PublishAck} is authoritative about that.
     * @return the number of gaps reported
     */
    public long gapCount() {
        return gapCount;
    }

    /**
     * The most recent gap the server reported for this batch, if any.
     * <p>
     * <b>Proposal.</b> ADR-50 delivers gaps to the client but does not say whether a client
     * should retain them, and no other client does. Retaining the last one lets code that
     * registered no {@link FastPublishListener} still see that the batch was interrupted and
     * where. Only the last is kept, because a {@link GapMode#Ok} batch has no bound on how
     * many there may be.
     * @return the last gap reported, or null if none was
     */
    @Nullable
    public FastFlowGap getLastGap() {
        return lastGap;
    }

    // ------------------------------------------------------------------------------------
    // publishing
    // ------------------------------------------------------------------------------------

    /**
     * Add a message to the batch. Blocks only when flow control says the client is too far ahead.
     * @param subject the subject
     * @param data the payload, may be null
     * @return where the batch stands after this message
     * @throws FastPublishException if the batch is finished or the server rejects the batch
     */
    public FastPubAck add(@NonNull String subject, byte[] data) throws FastPublishException {
        return add(subject, null, data);
    }

    /**
     * Add a message to the batch. Blocks only when flow control says the client is too far ahead.
     * @param subject the subject
     * @param userHeaders headers for this message, may be null
     * @param data the payload, may be null
     * @return where the batch stands after this message
     * @throws FastPublishException if the batch is finished or the server rejects the batch
     */
    public FastPubAck add(@NonNull String subject, Headers userHeaders, byte[] data) throws FastPublishException {
        // Read whatever is already queued before spending a batch sequence. A gap or error that
        // arrived since the last call ends the batch here, so _send's requireUsable throws
        // instead of one more message going out into a batch the server has already dropped.
        // On the first add this is necessarily empty - the inbox is fresh and nothing has been
        // published to it - so it costs one counter read.
        drain();

        boolean first = _send(subject, userHeaders, data,
            batchSeq == 0 ? FAST_BATCH_OP_START : FAST_BATCH_OP_APPEND) == 1;

        if (first) {
            awaitFirstReply();
        }

        drain();
        awaitFlowWindow();

        return new FastPubAck(batchSeq, lastAckSeq);
    }

    /**
     * Wait for the server's answer to the first message of the batch, which is mandatory: it is
     * how the client learns the feature exists on this server and this stream, and what flow
     * rate it may start at. A server that predates fast ingest silently ignores the {@code $FI}
     * reply subject and never answers, which is why the absence of a reply is itself the signal
     * and has to be bounded by the ack timeout.
     * @throws FastPublishException if nothing answers, or the answer is a server error
     */
    private void awaitFirstReply() throws FastPublishException {
        Message m = nextMessage();
        if (m == null) {
            // The batch never started, and the message that would have started it is already on
            // the wire, so the next add would append to a batch the server does not have. Give
            // up on it here rather than leaving a publisher that looks usable and is not, and
            // release the control channel with it: nothing is coming.
            abandon();
            throw new FastPublishException(batchId,
                "No response to the first message of the batch. The server may not support fast ingest publish.");
        }
        process(m);
    }

    /**
     * Block while the client is further ahead of the server than the number of acknowledgements
     * it is willing to have outstanding. This is the whole of the client's half of flow control:
     * the server states a rate, and the client refuses to run more than
     * {@code ackEvery * maxOutstandingAcks} messages past the last sequence the server confirmed.
     * Pings while it waits, so a lost acknowledgement does not stall the batch.
     * @throws FastPublishException if no acknowledgement arrives within the ack timeout
     */
    private void awaitFlowWindow() throws FastPublishException {
        while (!terminal && ackEvery > 0 && lastAckSeq + (ackEvery * maxOutstandingAcks) <= batchSeq) {
            Message m = nextMessagePinging();
            if (m == null) {
                throw new FastPublishException(batchId, "Timed out waiting for a flow acknowledgement.");
            }
            process(m);
        }
    }

    /**
     * Close the batch by publishing a final message, which is stored along with the rest.
     * ADR-50 operation 2, "Commit and store the final message".
     * @param subject the subject
     * @param data the payload, may be null
     * @return the authoritative PublishAck for the batch
     * @throws FastPublishException if the batch is finished or the server reports an error
     */
    public PublishAck closeBatch(@NonNull String subject, byte[] data) throws FastPublishException {
        return closeBatch(subject, null, data);
    }

    /**
     * Close the batch by publishing a final message, which is stored along with the rest.
     * ADR-50 operation 2, "Commit and store the final message".
     * @param subject the subject
     * @param userHeaders headers for this message, may be null
     * @param data the payload, may be null
     * @return the authoritative PublishAck for the batch
     * @throws FastPublishException if the batch is finished or the server reports an error
     */
    public PublishAck closeBatch(@NonNull String subject, Headers userHeaders, byte[] data) throws FastPublishException {
        requireClosable();
        _send(subject, userHeaders, data, FAST_BATCH_OP_COMMIT);
        try {
            return awaitPubAck(true);
        }
        finally {
            abandonIfCloseDidNotFinish();
        }
    }

    /**
     * Close the batch <b>without storing a final message</b>. ADR-50 operation 3, "Commit without
     * storing the final message (EOB mode)". The server does not store the sentinel, and the
     * returned {@link PublishAck} count excludes it. Use it when the batch is exactly the messages
     * already added, rather than holding one back to carry the close or inventing a filler.
     * <p>
     * The sentinel is published on the subject of the <b>first</b> message added. There is
     * deliberately no overload taking a subject: the sentinel must land on a subject the stream
     * captures, and the stream has already taken a message on the first added subject.
     * @return the authoritative PublishAck for the batch. Its batch size excludes the sentinel.
     * @throws FastPublishException if the batch is finished or has no messages in it
     */
    public PublishAck closeBatch() throws FastPublishException {
        requireClosable();
        if (firstSubject == null) {
            throw new FastPublishException(batchId, "Cannot close an empty batch without a final message");
        }
        _send(firstSubject, null, null, FAST_BATCH_OP_COMMIT_EOB);
        try {
            return awaitPubAck(true);
        }
        finally {
            abandonIfCloseDidNotFinish();
        }
    }

    /**
     * Ask the server to re-send the current flow state and report any gap. Does not consume a
     * batch sequence, which matters because incrementing would make a lost ping look like a gap
     * and fail a {@link GapMode#Fail} batch.
     * <p>
     * The ping is addressed to the subject of the <b>first</b> message in the batch, which the
     * stream is already known to capture since it has taken a message on it, and which is where
     * every other client sends it. A batch with no messages has no such subject and cannot be
     * pinged.
     * <p>
     * Not needed to keep the batch alive, which the publisher does on its own. What this adds is
     * the answer: the flow state and any gap are processed, and listener callbacks run, on the
     * calling thread before this returns, without publishing a message.
     * @throws FastPublishException if the batch is finished, or has no messages in it
     */
    public void ping() throws FastPublishException {
        requireUsable();
        if (firstSubject == null) {
            throw new FastPublishException(batchId, "Cannot ping a batch with no messages");
        }
        sendPing();
        Message m = nextMessage();
        if (m != null) {
            process(m);
        }
        drain();
    }

    /**
     * Give up on this batch without committing. The server cleans it up on its own inactivity
     * timeout. Anything already persisted stays persisted, and because there is no commit there
     * is no PublishAck, so there is no record of what that was.
     * <p>
     * Also used internally where a failure leaves a batch that can never be used again: a first
     * message the server never answered, and a commit whose acknowledgement never arrived.
     */
    public void abandon() {
        end(EndReason.Abandoned);
        abandoned = true;
        unsubscribe();
    }

    /**
     * Abandon the batch if it has not already ended, and release the control channel
     * subscription either way.
     * <p>
     * This never commits. try-with-resources calls it on the exception path as well as the
     * normal one, so a close that committed would commit a batch whose assembly had just
     * failed. A batch that already ended - committed, or stopped by a gap or error - is left
     * as it is and only its subscription is released, which matters because the gap and error
     * endings do not release it on their own. Calling it more than once is harmless.
     */
    @Override
    public void close() {
        if (!isTerminal()) {
            abandon();
        }
        unsubscribe();
    }

    // ------------------------------------------------------------------------------------
    // internals
    // ------------------------------------------------------------------------------------

    /**
     * Whether anything more is expected from the server. A batch that ended on a gap or an error
     * is terminal for adding but not finished: the server still owes the batch's final
     * PublishAck, and the drain has to keep reading for it.
     * @return true when nothing more will arrive
     */
    private boolean isFinished() {
        return abandoned || terminalAckReceived;
    }

    /**
     * Whether the server ended this batch under the client, rather than the client ending it.
     * @return true after a gap or an error ended the batch
     */
    private boolean serverEnded() {
        EndReason reason = endReason.get();
        return reason == EndReason.Gap || reason == EndReason.Error;
    }

    /**
     * Refuse a closeBatch the batch cannot take. When the server already ended the batch, the
     * exception carries the terminal PublishAck the server sent, because that ack is the only
     * authoritative record of what was persisted and refusing without it throws the record away.
     * @throws FastPublishException if the batch cannot be committed
     */
    private void requireClosable() throws FastPublishException {
        // Deliberately does not drain first. A batch the server ended goes through terminalAck(),
        // which reads and processes everything queued on its way to the terminal ack, so no
        // listener callback or counter is skipped. Draining here instead would also consume an
        // unsolicited PublishAck before the commit is sent, which is the one case where the
        // count check on that ack still has something to say.
        if (abandoned) {
            throw new FastPublishException(batchId, "Batch was abandoned.");
        }
        if (serverEnded()) {
            String what = endReason.get() == EndReason.Gap ? "a gap" : "an error";
            FastPublishException e = new FastPublishException(batchId,
                "Batch was ended by the server on " + what + ", so there is nothing to close.");
            e.setPublishAck(terminalAck());
            throw e;
        }
        requireUsable();
    }

    /**
     * End a batch whose closeBatch did not complete. The closing message is already on the wire, so
     * the batch is in a state only the server knows: it may have committed and lost the ack, or
     * never committed at all. Either way a second commit would be wrong, and the lost ack cannot
     * be recovered by pinging, because a committed batch is cleaned up server side and a ping
     * would be answered as an unknown batch. So the publisher is finished.
     * <p>
     * Does nothing when the commit ran to its acknowledgement, since that already ended the
     * batch, including when the acknowledgement carried a server error.
     */
    private void abandonIfCloseDidNotFinish() {
        if (!isTerminal()) {
            abandon();
        }
    }

    /**
     * Read until the terminal PublishAck of a server ended batch arrives. Returns null rather
     * than throwing when it does not: the caller is already being told the batch failed, and
     * ADR-50 makes these acks best effort, so a missing one is a thinner answer rather than a
     * second failure. An ack that carries a server error cannot be turned into a PublishAck at
     * all, which is the other way this returns null.
     * @return the ack, or null
     */
    private PublishAck terminalAck() {
        if (terminalAckReceived) {
            return finalAck; // null when the terminal ack carried an error
        }
        try {
            return awaitPubAck(false);
        }
        catch (FastPublishException e) {
            return null;
        }
        catch (IllegalStateException e) {
            // the subscription is already gone, so the ack can no longer arrive
            return null;
        }
        finally {
            unsubscribe();
        }
    }

    /**
     * Throw unless the batch can still take an operation.
     * @throws FastPublishException if the batch was abandoned or is already finished
     */
    private void requireUsable() throws FastPublishException {
        if (abandoned) {
            throw new FastPublishException(batchId, "Batch was abandoned.");
        }
        if (terminal) {
            throw new FastPublishException(batchId, "Batch is already finished.");
        }
    }

    /**
     * Everything a send does apart from the operation and what follows it. The sequence advances
     * before the publish because the reply subject carries it, and firstSubject is recorded after,
     * so a non-null firstSubject means at least one message actually reached the server.
     * @param subject the subject
     * @param userHeaders headers for this message
     * @param data the payload
     * @param op the fast batch operation code
     * @return the batch sequence this message was published with
     * @throws FastPublishException if the batch is no longer usable
     */
    private long _send(String subject, Headers userHeaders, byte[] data, String op) throws FastPublishException {
        requireUsable();
        // The EOB sentinel is the next message in the batch on the wire but is never stored, so
        // it takes the next sequence without batchSeq advancing. That keeps batchSeq meaning one
        // thing - the messages the batch will store - which is what size() reports and what the
        // pub ack's BatchSize counts.
        boolean eob = FAST_BATCH_OP_COMMIT_EOB.equals(op);
        boolean commit = eob || FAST_BATCH_OP_COMMIT.equals(op);
        if (commit) {
            // No idle ping may follow the commit: the commit's own wait pings, and a ping reaching
            // a batch the server has already cleaned up is answered as an unknown batch. Stopping
            // takes the lock, so an idle ping already being sent finishes before the commit goes.
            stopIdlePing();
        }
        long seq = eob ? batchSeq + 1 : ++batchSeq;
        try {
            conn.publish(subject, reply(seq, op), userHeaders, data);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            // jnats rejects the publish itself for an invalid subject, a closed or draining
            // connection, or a full reconnect buffer. A full reconnect buffer is the realistic
            // one here, since this path runs at rates that fill it during a reconnect. None of
            // them reaches the outgoing queue, so nothing left the client and the sequence is
            // given back - unless this was the EOB sentinel, which never took one.
            if (!eob) {
                --batchSeq;
            }
            if (commit) {
                resumeIdlePing(); // the batch is still open, and may be committed again
            }
            throw new FastPublishException(batchId, e);
        }
        lastSendNanos = System.nanoTime();
        if (firstSubject == null) {
            firstSubject = subject;
        }
        if (!commit) {
            sentSeq = seq; // after firstSubject, so the idle ping thread sees both
            if (seq == 1) {
                resumeIdlePing(); // the batch exists on the server from its first message
            }
        }
        return seq;
    }

    /**
     * Publish a ping for the highest batch sequence sent, on the publishing thread. The sequence
     * is not advanced, which matters because incrementing would make a lost ping look like a gap.
     * @throws FastPublishException if the connection refuses the publish
     */
    private void sendPing() throws FastPublishException {
        try {
            conn.publish(firstSubject, reply(batchSeq, FAST_BATCH_OP_PING), null, null);
        }
        catch (IllegalArgumentException | IllegalStateException e) {
            throw new FastPublishException(batchId, e);
        }
        lastSendNanos = System.nanoTime();
    }

    /**
     * Wait for the next control message for up to the ack timeout, pinging after each third of it
     * that passes with nothing, so at most two pings before giving up. A ping makes the server
     * resend its current flow state, which is how a lost flow acknowledgement is recovered.
     * The same third of the timeout, and the same two pings, as the C, Go, .NET, Python and Rust
     * clients.
     * @return the message, or null if nothing arrived within the ack timeout
     * @throws FastPublishException if interrupted, or a ping cannot be published
     */
    private Message nextMessagePinging() throws FastPublishException {
        long timeout = ackTimeout.toNanos();
        long third = timeout / 3;
        long interval = third < 1 ? 1 : third;
        long deadline = System.nanoTime() + timeout;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                Message m = control.poll(remaining < interval ? remaining : interval, TimeUnit.NANOSECONDS);
                if (m != null) {
                    return m;
                }
                if (deadline - System.nanoTime() > 0) {
                    sendPing();
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FastPublishException(batchId, e);
        }
    }

    /**
     * Start, or restart, pinging an idle batch. Does nothing when idle pings are turned off, or
     * are already scheduled.
     */
    private void resumeIdlePing() {
        if (idlePingNanos == 0) {
            return;
        }
        synchronized (idlePingLock) {
            idlePingStopped = false;
            if (idlePingTask == null) {
                idlePingTask = IDLE_PINGER.schedule(this::idlePing, idlePingNanos, TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Stop pinging an idle batch. Called when the commit goes out and whenever the batch ends.
     */
    private void stopIdlePing() {
        synchronized (idlePingLock) {
            idlePingStopped = true;
            if (idlePingTask != null) {
                idlePingTask.cancel(false);
                idlePingTask = null;
            }
        }
    }

    /**
     * Runs on the shared idle ping thread. Pings when nothing has been sent for the interval, and
     * schedules itself for the moment the batch will next have been idle that long, so the batch
     * never goes more than about one interval without a message.
     * <p>
     * It only publishes. The server's answer arrives on the control channel like any other and
     * is processed on the publishing thread, so no accounting or listener callback runs here.
     */
    private void idlePing() {
        synchronized (idlePingLock) {
            idlePingTask = null;
            if (idlePingStopped || isTerminal()) {
                return;
            }
            long idle = System.nanoTime() - lastSendNanos;
            if (idle >= idlePingNanos) {
                long seq = sentSeq; // the volatile read comes first, so firstSubject is visible
                try {
                    conn.publish(firstSubject, reply(seq, FAST_BATCH_OP_PING), null, null);
                }
                catch (IllegalArgumentException | IllegalStateException e) {
                    // A closed connection will never publish again, so stop. Anything else, a full
                    // reconnect buffer above all, is worth another try an interval from now.
                    if (conn.getStatus() == Connection.Status.CLOSED) {
                        return;
                    }
                }
                lastSendNanos = System.nanoTime();
                idle = 0;
            }
            idlePingTask = IDLE_PINGER.schedule(this::idlePing, idlePingNanos - idle, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Build the reply subject for one message. Every parameter the server needs is in it:
     * the inbox to answer on, the batch id, the flow rate the client is asking for, the gap
     * mode, the batch sequence and the operation.
     * @param seq the batch sequence
     * @param op the operation code
     * @return the reply subject
     */
    private String reply(long seq, String op) {
        return replyPrefix + seq + "." + op + "." + FAST_BATCH_SUFFIX;
    }

    /**
     * Read control messages until the authoritative PublishAck arrives.
     * @param ping whether to ping while waiting. True after this client sent a commit; false when
     *             collecting the terminal ack of a batch the server already ended, which a ping
     *             cannot help
     * @return the PublishAck
     * @throws FastPublishException on timeout or a server reported error
     */
    private PublishAck awaitPubAck(boolean ping) throws FastPublishException {
        try {
            while (true) {
                Message m = ping ? nextMessagePinging() : nextMessage();
                if (m == null) {
                    throw new FastPublishException(batchId, "Timed out waiting for the batch PublishAck.");
                }
                requireNotAStatus(m);
                JsonValue jv = parse(m);
                String type = jv == null ? null : type(jv);
                if (type == null) {
                    // no recognised flow type means this is the PublishAck, which is terminal
                    PublishAck pa = takeTerminalAck(m);
                    validateAck(pa);
                    return pa;
                }
                handle(type, jv);
            }
        }
        catch (JetStreamApiException e) {
            throw new FastPublishException(batchId, e);
        }
    }

    /**
     * Take the batch's terminal PublishAck, and record how the batch ended from what it says.
     * <p>
     * The ack is parsed before the batch is marked ended, because the ack decides the reason:
     * a PublishAck ends it as {@link EndReason#Committed}, and an error in its place - the server
     * refusing {@code closeBatch}, or answering a message for a batch it has already dropped with
     * {@code 10208 batch publish ID unknown} - ends it as {@link EndReason#Error}. Only the first
     * ending counts, so a batch the server already ended on a gap keeps {@link EndReason#Gap}
     * when its final ack arrives.
     * @param m the terminal ack message
     * @return the PublishAck
     * @throws FastPublishException if the ack cannot be read as one
     * @throws JetStreamApiException if the ack carries a server error
     */
    private PublishAck takeTerminalAck(Message m) throws FastPublishException, JetStreamApiException {
        terminalAckReceived = true;
        unsubscribe();
        try {
            finalAck = new PublishAck(m);
        }
        catch (IOException e) {
            // PublishAck makes an IOException when the ack is invalid, so nothing says the
            // batch was stored
            end(EndReason.Error);
            throw new FastPublishException(batchId, e.getMessage());
        }
        catch (JetStreamApiException e) {
            end(EndReason.Error);
            throw e;
        }
        end(EndReason.Committed);
        return finalAck;
    }

    /**
     * Check the server's account of the batch against the client's own. ADR-50 defines
     * {@code BatchSize} as the messages the batch stored, which is what {@link #size()} counts,
     * so the two must agree on a batch that ran to a clean commit.
     * <p>
     * Skipped once a gap or a per message error has been reported, because there the server
     * received less than the client sent and the client's count is an upper bound rather than
     * an equal. Failing on that difference would fail every gap abandoned batch.
     * @param pa the terminal PublishAck
     * @throws FastPublishException if the server's account disagrees with the client's
     */
    private void validateAck(PublishAck pa) throws FastPublishException {
        if (gapCount > 0 || pendingError != null) {
            return;
        }
        if (pa.getBatchSize() != batchSeq) {
            throw new FastPublishException(batchId,
                "The server reported " + pa.getBatchSize() + " messages in the batch, the client sent " + batchSeq + ".");
        }
        if (!batchId.equals(pa.getBatchId())) {
            throw new FastPublishException(batchId,
                "The server reported batch id " + pa.getBatchId() + ".");
        }
    }

    /**
     * Runs on the dispatcher's thread, and does the least it can: work out whether this message
     * ends the batch, record that, and hand the message to the publishing thread.
     * <p>
     * Recording it here is the whole point of being asynchronous. A batch the server abandons
     * while the application is between publishes is known to be over immediately, rather than at
     * the next {@code add}. Everything else - the flow accounting, the gap and error counters,
     * the terminal ack, every listener callback - deliberately stays on the thread that called
     * {@code add}, {@code closeBatch} or {@code ping}, and still happens in arrival order, because
     * the dispatcher delivers serially and the queue preserves that order.
     * @param m the control message
     */
    private void onControlMessage(Message m) {
        classify(m);
        control.add(m);
    }

    /**
     * Decide whether a control message ends the batch, without doing any of the work that
     * follows from it.
     * @param m the control message
     */
    private void classify(Message m) {
        if (gapMode != GapMode.Fail) {
            return; // in Ok mode nothing the server reports ends the batch
        }
        JsonValue jv = parse(m);
        String type = jv == null ? null : type(jv);
        if (FAST_BATCH_TYPE_GAP.equals(type)) {
            end(EndReason.Gap);
        }
        else if (FAST_BATCH_TYPE_ERR.equals(type)) {
            end(EndReason.Error);
        }
        // A PublishAck is deliberately not classified here. It is normally the answer to a commit
        // this client just sent, so the publishing thread is already on its way to reading it,
        // and ending the batch from here would only race that. The case this method exists for is
        // the other one: the server abandoning the batch under an application that has gone quiet.
    }

    /**
     * End the batch, keeping the first reason. Called from the dispatcher thread when a message
     * arrives and from the publishing thread when the same message is processed, so it has to be
     * idempotent and has to keep the earlier answer: a gap ended batch still receives a terminal
     * PublishAck afterwards, and collecting that ack must not relabel it as committed.
     * @param reason why the batch ended
     */
    private void end(EndReason reason) {
        // The reason is set first on purpose. Both fields are written here and read elsewhere,
        // and only one order is safe to read: a caller that sees isTerminal() true must never
        // then see Open, because that is the pair it acts on. The volatile write to terminal
        // cannot be reordered before the compare and set, so seeing the flag guarantees seeing
        // the reason. The reverse window - a reason set an instant before the flag - is
        // harmless, and in fact useful, since serverEnded() noticing early only means the
        // terminal ack is collected sooner.
        endReason.compareAndSet(EndReason.Open, reason);
        terminal = true;
        stopIdlePing();
    }

    private Message nextMessage() throws FastPublishException {
        try {
            return control.poll(ackTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FastPublishException(batchId, e);
        }
    }

    /**
     * Process whatever the dispatcher has already handed over, and nothing more. A poll with no
     * timeout on the internal queue returns null the moment it is empty, which is the common
     * case on this path: it runs twice per add and the control channel is quiet by design, one
     * acknowledgement per flow window rather than one per message.
     */
    private void drain() throws FastPublishException {
        while (!isFinished()) {
            Message m = control.poll();
            if (m == null) {
                return;
            }
            process(m);
        }
    }

    /**
     * Refuse a status message on the control channel. The server answers `503 No Responders` on
     * the reply subject when the subject a message went to has no subscriber at all, which for a
     * fast batch means the stream does not capture it - a misconfigured subject rather than
     * anything about the batch. It is not an acknowledgement and must not be read as one: doing
     * so ends the batch as committed and reports "Invalid JetStream ack", neither of which is
     * what happened. The batch cannot proceed either way, so it is given up here.
     * @param m the control message
     * @throws FastPublishException always, when the message is a status
     */
    private void requireNotAStatus(Message m) throws FastPublishException {
        if (m.isStatusMessage()) {
            abandon();
            throw new FastPublishException(batchId,
                "The server answered the batch with a status rather than an acknowledgement: "
                    + m.getStatus() + ". The stream may not capture the subject being published to.");
        }
    }

    private void process(Message m) throws FastPublishException {
        requireNotAStatus(m);
        JsonValue jv = parse(m);
        String type = jv == null ? null : type(jv);
        if (type == null) {
            // a PubAck arrived early, which means the batch is over. It is kept rather than
            // discarded: for a batch the server ended on a gap this is the only authoritative
            // record of what was persisted, and the caller asks for it at closeBatch.
            try {
                takeTerminalAck(m);
            }
            catch (JetStreamApiException e) {
                throw new FastPublishException(batchId, e);
            }
            return;
        }
        handle(type, jv);
    }

    private void handle(String type, JsonValue jv) {
        if (FAST_BATCH_TYPE_ACK.equals(type)) {
            // acks are cumulative and can arrive out of order, so only ever move forward
            long seq = readLong(jv, SEQ, 0);
            if (seq > lastAckSeq) {
                lastAckSeq = seq;
            }
            long msgs = readLong(jv, MSGS, 0);
            if (msgs > 0 && msgs != ackEvery) {
                ackEvery = msgs;
                listener.onFlowChange(msgs);
            }
        }
        else if (FAST_BATCH_TYPE_GAP.equals(type)) {
            // never touch lastAckSeq or ackEvery from a gap. Gaps are sent on detection and so
            // arrive out of order with respect to flow acks.
            lastGap = new FastFlowGap(jv);
            gapCount++;
            // recorded before the callback so a listener that queries the publisher sees it
            listener.onGap(lastGap);
            if (gapMode == GapMode.Fail) {
                end(EndReason.Gap);
            }
        }
        else if (FAST_BATCH_TYPE_ERR.equals(type)) {
            pendingError = new FastFlowError(jv);
            listener.onError(pendingError);
            if (gapMode == GapMode.Fail) {
                end(EndReason.Error);
            }
        }
    }

    private static JsonValue parse(Message m) {
        byte[] data = m.getData();
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return JsonParser.parse(data);
        }
        catch (JsonParseException e) {
            return null;
        }
    }

    private static String type(JsonValue jv) {
        // A PubAck has no type field, so "no recognised flow type" is a safe discriminator.
        return readString(jv, TYPE);
    }

    private void unsubscribe() {
        try {
            // the subscription belongs to the dispatcher and refuses to be unsubscribed on its
            // own, so closing the dispatcher is the teardown: it drops the subscription and
            // releases the thread.
            conn.closeDispatcher(dispatcher);
        }
        catch (IllegalArgumentException | IllegalStateException ignore) {
            // already closed, or the connection is gone. closeDispatcher throws
            // IllegalArgumentException for a dispatcher it has already released, and this is
            // reached more than once by design - abandon, close and the terminal ack all release.
        }
    }

    /**
     * Get an instance of the builder, same as new FastPublisher.Builder();
     * @return The Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the FastPublisher
     */
    public static class Builder {
        /**
         * Construct a builder with the default settings.
         */
        public Builder() {}

        Connection conn;
        Duration ackTimeout;
        String batchId;
        GapMode gapMode = GapMode.Fail;
        int maxFlow = DEFAULT_MAX_FLOW;
        int maxOutstandingAcks = DEFAULT_MAX_OUTSTANDING_ACKS;
        FastPublishListener listener = new FastPublishListener() {};
        Integer idlePingSeconds; // null until set, so build can tell unset from turned off

        /**
         * The connection to publish on. Required.
         * @param conn the connection
         * @return The Builder
         */
        public Builder connection(Connection conn) {
            this.conn = conn;
            return this;
        }

        /**
         * The batch id. Generated when not supplied. Cannot be longer than 64 characters.
         * @param batchId the batch id
         * @return The Builder
         */
        public Builder batchId(String batchId) {
            this.batchId = batchId;
            return this;
        }

        /**
         * How gaps are handled. Defaults to {@link GapMode#Fail}, which keeps a gap from silently
         * becoming a hole in the data.
         * @param gapMode the gap mode
         * @return The Builder
         */
        public Builder gapMode(GapMode gapMode) {
            this.gapMode = gapMode == null ? GapMode.Fail : gapMode;
            return this;
        }

        /**
         * The upper bound on how many messages the server may go between acknowledgements.
         * The server starts lower and works up toward this.
         * Less than 1 means use {@value #DEFAULT_MAX_FLOW}; anything above
         * {@value #MAX_FLOW_CEILING} is capped there.
         * @param maxFlow the maximum flow
         * @return The Builder
         */
        public Builder maxFlow(int maxFlow) {
            this.maxFlow = maxFlow < 1 ? DEFAULT_MAX_FLOW : Math.min(maxFlow, MAX_FLOW_CEILING);
            return this;
        }

        /**
         * How many acknowledgements the client is willing to have outstanding before it blocks.
         * 1 is fully lockstep, throttled to one flow window at a time;
         * {@value #DEFAULT_MAX_OUTSTANDING_ACKS} is the ADR-50 recommendation and suits most
         * cases; {@value #MAX_OUTSTANDING_ACKS} helps on higher latency links.
         * Less than 1 means use {@value #DEFAULT_MAX_OUTSTANDING_ACKS}; anything above
         * {@value #MAX_OUTSTANDING_ACKS} is capped there.
         * @param maxOutstandingAcks the maximum outstanding acks
         * @return The Builder
         */
        public Builder maxOutstandingAcks(int maxOutstandingAcks) {
            this.maxOutstandingAcks = maxOutstandingAcks < 1 ? DEFAULT_MAX_OUTSTANDING_ACKS : Math.min(maxOutstandingAcks, MAX_OUTSTANDING_ACKS);
            return this;
        }

        /**
         * How long to wait, in milliseconds, for a flow acknowledgement or the final PublishAck.
         * Less than 1 means use the connection's timeout. Milliseconds rather than a Duration
         * because no timeout below a millisecond is reasonable, and because jnats reads a
         * duration under one nanosecond as wait forever, which would turn every wait in this
         * publisher into a hang - the opposite of what the timeout is for.
         * @param ackTimeoutMillis the ack timeout in milliseconds
         * @return The Builder
         */
        public Builder ackTimeout(long ackTimeoutMillis) {
            this.ackTimeout = ackTimeoutMillis < 1 ? null : Duration.ofMillis(ackTimeoutMillis);
            return this;
        }

        /**
         * How many seconds the batch may go without a message before the publisher pings it to
         * keep it alive. The server abandons a batch that receives nothing for
         * {@link BatchUtils#getBatchIdleTimeoutSeconds(Connection)} seconds, 10 by default, so
         * without a ping a batch whose application pauses that long is lost, and its {@code closeBatch} is
         * answered {@code 10208 batch publish ID unknown}.
         * <p>
         * When not set, {@link BatchUtils#getDefaultIdlePingSeconds(Connection)}, which is
         * {@value BatchUtils#DEFAULT_IDLE_PING_PERCENT}% of the server timeout: 5 seconds today.
         * Less than 1 means the publisher does not ping an idle batch at all, which leaves keeping
         * it alive to the application through {@link FastPublisher#ping()}. More than
         * {@link BatchUtils#getMaxIdlePingSeconds(Connection)},
         * {@value BatchUtils#MAX_IDLE_PING_PERCENT}% of the server timeout and so 8 seconds today,
         * is refused by {@code build()}, because it leaves too little room for one ping to be
         * late or lost.
         * @param idlePingSeconds the idle ping interval in seconds
         * @return The Builder
         */
        public Builder idlePingSeconds(int idlePingSeconds) {
            this.idlePingSeconds = idlePingSeconds;
            return this;
        }

        /**
         * Where gap, error and flow change reports are delivered.
         * @param listener the listener
         * @return The Builder
         */
        public Builder listener(FastPublishListener listener) {
            this.listener = listener == null ? new FastPublishListener() {} : listener;
            return this;
        }

        private void validateAndDefault() {
            validateNotNull(conn, "Connection required,");
            // There is no useful server side error to fall back on: a pre 2.14 server treats the
            // $FI reply subject as an ordinary reply and never answers, so the client would just
            // hang until ackTimeout.
            if (!conn.getServerInfo().isNewerVersionThan("2.13.99")) {
                throw new IllegalArgumentException("Fast ingest publish not available until server version 2.14.0.");
            }
            if (ackTimeout == null) {
                ackTimeout = conn.getOptions().getConnectionTimeout();
            }
            if (idlePingSeconds == null) {
                idlePingSeconds = BatchUtils.getDefaultIdlePingSeconds(conn);
            }
            else if (idlePingSeconds < 1) {
                idlePingSeconds = 0;
            }
            else {
                int max = BatchUtils.getMaxIdlePingSeconds(conn);
                if (idlePingSeconds > max) {
                    throw new IllegalArgumentException("Idle ping seconds cannot be more than " + max
                        + ", " + BatchUtils.MAX_IDLE_PING_PERCENT + "% of the server's "
                        + BatchUtils.getBatchIdleTimeoutSeconds(conn) + " second batch idle timeout.");
                }
            }
            batchId = emptyAsNull(batchId);
            if (batchId == null) {
                batchId = new NUID().next();
            }
            else if (batchId.length() > 64) {
                throw new IllegalArgumentException("Batch ID cannot be longer than 64 characters");
            }
            else {
                // The id is one token of the reply subject the server parses right to left, so a
                // dot would leave it reading only the last segment as the id, and a wildcard or a
                // space would make the subject invalid.
                validatePrintableExceptWildDotGt(batchId, "Batch ID", true);
            }
        }

        /**
         * Build the publisher. This does not contact the server; feature detection happens on
         * the first add.
         * @return the publisher
         */
        public FastPublisher build() {
            validateAndDefault();
            return new FastPublisher(this);
        }
    }
}
