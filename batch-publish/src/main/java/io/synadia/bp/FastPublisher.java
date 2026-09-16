// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import org.jspecify.annotations.NonNull;

import static io.nats.client.support.NatsJetStreamConstants.FAST_BATCH_OP_COMMIT;

/**
 * Publishes a fast ingest batch and ends it by sending a final real message that is stored
 * along with the rest.
 * <p>
 * Use {@link EobFastPublisher} instead when the batch is exactly the messages you already have
 * and you do not want to hold one back, or invent a filler message, just to carry the commit.
 * <p>
 * Requires a server at 2.14.0 or later and a stream configured with {@code allow_batched}.
 * See {@link AbstractFastPublisher} for what fast ingest gives up in exchange for throughput.
 */
public class FastPublisher extends AbstractFastPublisher {

    private FastPublisher(Builder b) {
        super(b);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param data the payload, may be null
     * @return the authoritative PublishAck for the batch
     * @throws FastPublishException if the batch is finished or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, byte[] data) throws FastPublishException {
        return commit(subject, null, data);
    }

    /**
     * Publish the final message and commit the batch.
     * @param subject the subject
     * @param userHeaders headers for this message, may be null
     * @param data the payload, may be null
     * @return the authoritative PublishAck for the batch
     * @throws FastPublishException if the batch is finished or the server reports an error
     */
    public PublishAck commit(@NonNull String subject, Headers userHeaders, byte[] data) throws FastPublishException {
        requireCommittable();
        _send(subject, userHeaders, data, FAST_BATCH_OP_COMMIT);
        try {
            return awaitPubAck();
        }
        finally {
            abandonIfCommitDidNotFinish();
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
    public static class Builder extends AbstractFastPublisher.Builder<Builder, FastPublisher> {
        /**
         * Construct a builder with the default settings.
         */
        public Builder() {}

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public FastPublisher build() {
            validateAndDefault();
            return new FastPublisher(this);
        }
    }
}
