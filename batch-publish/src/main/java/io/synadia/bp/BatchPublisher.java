// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.nats.client.support.NatsJetStreamConstants.NATS_BATCH_COMMIT_STORE;

/**
 * Publishes an atomic batch, a group of up to 1000 messages that are all added to the stream or
 * none are, and ends it by sending a final real message that is stored along with the rest.
 * <p>
 * Use {@link EobBatchPublisher} instead when the batch is exactly the messages you already have
 * and you do not want to hold one back, or invent a filler message, just to carry the commit.
 * <p>
 * Requires a server at 2.12.0 or later and a stream configured with {@code allow_atomic}.
 */
public class BatchPublisher extends AbstractBatchPublisher {

    private BatchPublisher(Builder b) {
        super(b);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param data the payload
     * @return the PublishAck
     * @throws BatchPublishException if the batch is not open or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, byte[] data) throws BatchPublishException {
        return commit(subject, null, data, null);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param data the payload
     * @param opts per message options
     * @return the PublishAck
     * @throws BatchPublishException if the batch is not open or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        return commit(subject, null, data, opts);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param userHeaders headers for the final message
     * @param data the payload
     * @return the PublishAck
     * @throws BatchPublishException if the batch is not open or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, Headers userHeaders, byte[] data) throws BatchPublishException {
        return commit(subject, userHeaders, data, null);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param userHeaders headers for the final message
     * @param data the payload
     * @param opts per message options
     * @return the PublishAck
     * @throws BatchPublishException if the batch is not open or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) throws BatchPublishException {
        requireOpen();
        requireUserHeadersAllowed(userHeaders);
        requireExpectedLastSequenceOnlyOnFirst(opts);
        try {
            ++lastSeq;
            Message m = request(subject, userHeaders, data, NATS_BATCH_COMMIT_STORE, opts);
            PublishAck pa = new PublishAck(m);
            validateAck(pa);
            return pa;
        }
        catch (NotSent e) {
            // the commit message never left the client, so give its sequence back
            --lastSeq;
            throw e;
        }
        catch (IOException e) {
            // done this way because PublishAck makes an IOException if the ack is invalid.
            // it was done that way because of api backward compatibility
            // just no need of the extra layer
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
     * Publish the final message and commit the batch, asynchronously.
     * @param subject the subject
     * @param data the payload
     * @return a future for the PublishAck
     */
    public CompletableFuture<PublishAck> commitAsync(@NonNull String subject, byte[] data) {
        return commitAsync(subject, null, data, null);
    }

    /**
     * Publish the final message and commit the batch, asynchronously.
     * @param subject the subject
     * @param data the payload
     * @param opts per message options
     * @return a future for the PublishAck
     */
    public CompletableFuture<PublishAck> commitAsync(@NonNull String subject, byte[] data, BatchPublishOptions opts) {
        return commitAsync(subject, null, data, opts);
    }

    /**
     * Publish the final message and commit the batch, asynchronously.
     * @param subject the subject
     * @param userHeaders headers for the final message
     * @param data the payload
     * @return a future for the PublishAck
     */
    public CompletableFuture<PublishAck> commitAsync(@NonNull String subject, Headers userHeaders, byte[] data) {
        return commitAsync(subject, userHeaders, data, null);
    }

    /**
     * Publish the final message and commit the batch, asynchronously.
     * @param subject the subject
     * @param userHeaders headers for the final message
     * @param data the payload
     * @param opts per message options
     * @return a future for the PublishAck
     */
    public CompletableFuture<PublishAck> commitAsync(@NonNull String subject, Headers userHeaders, byte[] data, BatchPublishOptions opts) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return commit(subject, userHeaders, data, opts);
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
     * Get an instance of the builder, same as new BatchPublisher.Builder();
     * @return The Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the BatchPublisher
     */
    public static class Builder extends AbstractBatchPublisher.Builder<Builder, BatchPublisher> {
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
            return "2.11.99";
        }

        @Override
        protected String tooOldMessage() {
            return "Batch publish not available until server version 2.12.0.";
        }

        @Override
        public BatchPublisher build() {
            validateAndDefault();
            return new BatchPublisher(this);
        }
    }
}
