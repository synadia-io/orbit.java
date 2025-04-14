// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.synadia.retrier.RetryConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A class to config how publish retries are executed.
 */
public class PublishRetryConfig {

    public static final int DEFAULT_ATTEMPTS;
    public static final long[] DEFAULT_BACKOFF_POLICY;
    public static final List<RetryCondition> DEFAULT_RETRY_CONDITIONS;
    public static final PublishRetryConfig DEFAULT_CONFIG;

    static {
        // DEV NOTE. ORDER MATTERS. WE NEED THESE FOR THE CONFIG BUILDER
        DEFAULT_ATTEMPTS = RetryConfig.DEFAULT_ATTEMPTS;
        DEFAULT_BACKOFF_POLICY = RetryConfig.DEFAULT_BACKOFF_POLICY;
        List<RetryCondition> list = new ArrayList<>();
        list.add(RetryCondition.IoEx);
        list.add(RetryCondition.NoResponders);
        DEFAULT_RETRY_CONDITIONS = Collections.unmodifiableList(list);

        // CONFIG BUILDER LAST
        DEFAULT_CONFIG = PublishRetryConfig.builder().build();
    }

    public final RetryConfig retryConfig;
    public final boolean retryAll;
    public final boolean retryOnNoResponders;
    public final boolean retryOnIoEx;
    public final boolean retryOnJetStreamApiEx;
    public final boolean retryOnRuntimeEx;

    public PublishRetryConfig(RetryConfig retryConfig, List<RetryCondition> retryConditions) {
        this.retryConfig = retryConfig;
        this.retryOnNoResponders = retryConditions.contains(RetryCondition.NoResponders);
        this.retryOnIoEx = retryConditions.contains(RetryCondition.IoEx);
        this.retryOnJetStreamApiEx = retryConditions.contains(RetryCondition.JetStreamApiEx);
        this.retryOnRuntimeEx = retryConditions.contains(RetryCondition.RuntimeEx);
        retryAll = retryOnNoResponders && retryOnIoEx && retryOnJetStreamApiEx && retryOnRuntimeEx;
    }

    /**
     * Creates a builder for the config.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The builder class for the RetryConfig
     */
    public static class Builder {
        RetryConfig.Builder rcb = RetryConfig.builder();
        List<RetryCondition> retryConditions = new ArrayList<>();

        public Builder() {
            retryConditions.add(RetryCondition.NoResponders);
            retryConditions.add(RetryCondition.IoEx);
        }

        /**
         * Set the backoff policy
         * @param backoffPolicy the policy array
         * @return the builder
         */
        public Builder backoffPolicy(long[] backoffPolicy) {
            rcb.backoffPolicy(backoffPolicy);
            return this;
        }

        /**
         * Set the number of times to retry
         * @param attempts the number of retry attempts
         * @return the builder
         */
        public Builder attempts(int attempts) {
            rcb.attempts(attempts);
            return this;
        }

        /**
         * Set the deadline. The retry will be given at max this much time to execute
         * if expires before the number of retries.
         * @param retryDeadlineMillis the deadline time in millis
         * @return the builder
         */
        public Builder deadline(long retryDeadlineMillis) {
            rcb.deadline(retryDeadlineMillis);
            return this;
        }

        /**
         * Set the deadline by duration. Will be truncated to millis.
         * The retry will be given at max this much time to execute
         * if expires before the number of retries.
         * @param retryDeadline the deadline duration
         * @return the builder
         */
        public Builder deadline(Duration retryDeadline) {
            rcb.deadline(retryDeadline.toMillis());
            return this;
        }

        /**
         * Set the exception conditions where the publisher allows the retrier to continue
         * @param retryConditions the conditions
         * @return the builder
         */
        public Builder retryConditions(RetryCondition... retryConditions) {
            List<RetryCondition> temp = new ArrayList<>();
            if (retryConditions != null) {
                for (RetryCondition rc : retryConditions) {
                    if (rc != null) {
                        temp.add(rc);
                    }
                }
            }
            if (temp.isEmpty()) {
                this.retryConditions = DEFAULT_RETRY_CONDITIONS;
            }
            else {
                this.retryConditions = temp;
            }
            return this;
        }

        /**
         * Builds the retry config.
         * @return RetryConfig instance
         */
        public PublishRetryConfig build() {
            return new PublishRetryConfig(rcb.build(), retryConditions);
        }
    }
}
