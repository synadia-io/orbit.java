// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

/**
 * The Retrier is designed to give generic retry ability to retry anything.
 * There are also static methods which are use the generic ability that are built specifically for JetStream publishing.
 */
public class Retrier {
    private Retrier() {}  /* ensures cannot be constructed */

    /**
     * Execute the supplied action with the given retry config.
     * @param config The custom retry config
     * @param action The retry action
     * @return an instance of the return type
     * @param <T> the return type
     * @throws Exception various execution exceptions; only thrown if all retries failed.
     */
    public static <T> T execute(RetryConfig config, RetryAction<T> action) throws Exception {
        return execute(config, action, e -> true);
    }

    /**
     * Execute the supplied action with the given retry config.
     * @param config The custom retry config
     * @param action The retry action
     * @param observer The retry observer
     * @return an instance of the return type
     * @param <T> the return type
     * @throws Exception various execution exceptions; only thrown if all retries failed
     * or the observer declines to retry.
     */
    public static <T> T execute(RetryConfig config, RetryAction<T> action, RetryObserver observer) throws Exception {
        long[] backoffPolicy = config.getBackoffPolicy();;
        int plen = backoffPolicy.length;
        int retries = 0;
        long deadlineExpiresAt = System.currentTimeMillis() + config.getDeadline();
        if (deadlineExpiresAt < System.currentTimeMillis()) {
            deadlineExpiresAt = Long.MAX_VALUE;
        }

        while (true) {
            try {
                return action.execute();
            }
            catch (Exception e) {
                if (++retries <= config.getAttempts() && deadlineExpiresAt > System.currentTimeMillis() && observer.shouldRetry(e)) {
                    try {
                        int ix = retries - 1;
                        long sleep = ix < backoffPolicy.length ? backoffPolicy[ix] : backoffPolicy[plen-1];
                        //noinspection BusyWait
                        Thread.sleep(sleep);
                        continue; // goes back to start of while
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
                throw e;
            }
        }
    }
}
