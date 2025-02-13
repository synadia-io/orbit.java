// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.retrier;

/**
 * The action to execute with retry.
 * @param <T> The return type of the action
 */
public interface RetryAction<T> {
    /**
     * Execute the action
     * @return the result
     * @throws Exception various execution exceptions; The execution throws the last exception
     * if all retries failed or the observer declines to retry.
     */
    T execute() throws Exception;
}
