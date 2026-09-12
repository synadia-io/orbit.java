// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.api.PublishAck;

import static io.nats.client.support.NatsJetStreamConstants.FAST_BATCH_OP_COMMIT_EOB;

/**
 * Publishes a fast ingest batch and ends it <b>without storing a final message</b>.
 * <p>
 * ADR-50 calls this a commit: operation "Commit without storing the final message (EOB mode)".
 * The server does not store the sentinel, and the returned {@link PublishAck} count excludes it.
 * Use it when the batch is exactly the messages you already published, rather than holding one
 * message back to carry the commit or inventing a filler message to store.
 * <p>
 * Use {@link FastPublisher} instead when the last thing you have to publish is genuinely the
 * last message of the batch.
 * <p>
 * Requires a server at 2.14.0 or later and a stream configured with {@code allow_batched}.
 * See {@link AbstractFastPublisher} for what fast ingest gives up in exchange for throughput.
 */
public class EobFastPublisher extends AbstractFastPublisher {

    private EobFastPublisher(Builder b) {
        super(b);
    }

    /**
     * Commit the batch without storing a final message.
     * <p>
     * The sentinel is published on the subject of the <b>first</b> message added. There is
     * deliberately no overload taking a subject: the sentinel must land on a subject the stream
     * captures, and the first added subject is one by construction.
     * @return the authoritative PublishAck for the batch. Its batch size excludes the sentinel.
     * @throws FastPublishException if the batch is finished or has no messages in it
     */
    public PublishAck commit() throws FastPublishException {
        requireCommittable();
        if (firstSubject == null) {
            throw new FastPublishException(batchId, "Cannot commit an empty batch");
        }
        _send(firstSubject, null, null, FAST_BATCH_OP_COMMIT_EOB);
        return awaitPubAck();
    }

    /**
     * Get an instance of the builder, same as new EobFastPublisher.Builder();
     * @return The Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the EobFastPublisher
     */
    public static class Builder extends AbstractFastPublisher.Builder<Builder, EobFastPublisher> {
        /**
         * Construct a builder with the default settings.
         */
        public Builder() {}

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public EobFastPublisher build() {
            validateAndDefault();
            return new EobFastPublisher(this);
        }
    }
}
