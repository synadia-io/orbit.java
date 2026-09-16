// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.nats.client.support.NatsJetStreamConstants.NATS_BATCH_COMMIT_EOB;

/**
 * Publishes an atomic batch and ends it <b>without storing a final message</b>.
 * <p>
 * The commit is normally a header on a real message, so committing requires having a real message
 * to send. If your transaction is exactly five KV writes, you would either have to hold the fifth
 * write back and use it as the commit trigger, which couples "am I done?" to "do I have one more
 * message?", or invent a filler message and permanently store a piece of junk in the stream.
 * This publisher solves that: {@link #commit()} sends an end-of-batch sentinel that the server
 * does not store. The server rewrites the header of the previously received last message so the
 * batch commits normally, and the returned {@link PublishAck} count excludes the sentinel.
 * <p>
 * Use {@link BatchPublisher} instead when the last thing you have to publish is genuinely the
 * last message of your transaction.
 * <p>
 * Requires a server at 2.14.0 or later and a stream configured with {@code allow_atomic}.
 */
public class EobBatchPublisher extends AbstractBatchPublisher {

    private EobBatchPublisher(Builder b) {
        super(b);
    }

    /**
     * Commit the batch without storing a final message.
     * <p>
     * ADR-50 calls this a commit: operation "Commit without storing the final message (EOB mode)".
     * The sentinel is published on the subject of the <b>first</b> message added. There is
     * deliberately no overload taking a subject: the sentinel must land on a subject the stream
     * captures, and the stream has already taken a message on the first added subject.
     * @return the PublishAck. Its batch size excludes the sentinel.
     * @throws BatchPublishException if the batch is not open or has no messages in it
     */
    public PublishAck commit() throws BatchPublishException {
        requireOpen();
        if (firstSubject == null) {
            throw new BatchPublishException(batchId, "Cannot commit an empty batch");
        }
        try {
            // the sentinel carries only the 3 batch headers. ADR-50 excludes it from the header
            // check loop, so user headers and options here would silently do nothing.
            Message m = request(firstSubject, null, null, NATS_BATCH_COMMIT_EOB, null);
            PublishAck pa = new PublishAck(m);
            validateAck(pa);
            return pa;
        }
        catch (IOException e) {
            // done this way because PublishAck makes an IOException if the ack is invalid.
            throw new BatchPublishException(batchId, e.getMessage());
        }
        catch (JetStreamApiException e) {
            throw new BatchPublishException(batchId, e);
        }
        finally {
            markClosed();
        }
    }

    /**
     * Commit the batch without storing a final message, asynchronously.
     * @return a future for the PublishAck
     */
    public CompletableFuture<PublishAck> commitAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return commit();
            }
            catch (BatchPublishException e) {
                // CompletableFuture treats CompletionException as transport rather than cause:
                // supplyAsync stores it as is and get() unwraps it, so the caller's
                // ExecutionException.getCause() is this BatchPublishException itself.
                throw new CompletionException(e);
            }
        }, conn.getOptions().getExecutor());
    }

    /**
     * Get an instance of the builder, same as new EobBatchPublisher.Builder();
     * @return The Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the EobBatchPublisher
     */
    public static class Builder extends AbstractBatchPublisher.Builder<Builder, EobBatchPublisher> {
        /**
         * Construct a builder with the default settings.
         */
        public Builder() {}

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected String newerThanVersion() {
            // EOB is 2.14, later than plain atomic batch publish, so this gate is stricter.
            return "2.13.99";
        }

        @Override
        protected String tooOldMessage() {
            return "EOB batch publish not available until server version 2.14.0.";
        }

        @Override
        public EobBatchPublisher build() {
            validateAndDefault();
            return new EobBatchPublisher(this);
        }
    }
}
