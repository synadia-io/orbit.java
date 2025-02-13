// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.retrier;

/**
 * The RetryObserver gives an opportunity to inspect an exception
 * and optionally stop retries before the retry config is exhausted
 */
public interface RetryObserver {
    /**
     * Inspect the exception to determine if the execute should retry.
     * @param e the exception that occurred when executing the action.
     * @return true if the execution should retry (assuming that the retry config is not exhausted)
     */
    boolean shouldRetry(Exception e);
}
