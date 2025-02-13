// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.retrier.Retrier;
import io.synadia.retrier.RetryAction;
import io.synadia.retrier.RetryConfig;
import io.synadia.retrier.RetryObserver;

public class RetrierExample {

    private static final String RECOVERABLE = "recoverable";
    private static final String UNRECOVERABLE = "unrecoverable";

    public static void main(String[] args) {
        RetryConfig retryConfig = RetryConfig.builder()
            .attempts(5)
            .deadline(5000)
            .backoffPolicy(new long[]{100, 200, 300, 400, 500})
            .build();

        System.out.println("In this example, the action fails a few times, but eventually succeeds");
        ExampleActionProvider actionProvider = new ExampleActionProvider(3);
        ExampleRetryObserver observer = new ExampleRetryObserver();
        try {
            Retrier.execute(retryConfig, actionProvider, observer);
        }
        catch (Exception e) {
            System.out.println("This was setup to eventually complete, should not get here");
            System.exit(-1);
        }
        System.out.println("  Expecting 4 action executions: " + actionProvider.executions);
        System.out.println("  Expecting 0 failuresLeft: " + actionProvider.failuresLeft);
        System.out.println("  Expecting to observe 3 exceptions: " + observer.exceptions);

        // ==========================================================================================

        System.out.println("\nIn this example, the action fails more times than attempts allowed");
        actionProvider = new ExampleActionProvider(6);
        observer = new ExampleRetryObserver();
        try {
            Retrier.execute(retryConfig, actionProvider, observer);
        }
        catch (Exception e) {
            System.out.println("This was expected, even though it is \"recoverable\" -> " + e);
        }
        System.out.println("  Expecting 6 action executions: " + actionProvider.executions);
        System.out.println("  Expecting 1 failuresLeft: " + actionProvider.failuresLeft);
        System.out.println("  Expecting to observe 5 exceptions: " + observer.exceptions);

        // ==========================================================================================

        System.out.println("\nIn this example, there is an unrecoverable exception, so the observer cancelled the retry.");
        ExampleUnrecoverableActionProvider unProvider = new ExampleUnrecoverableActionProvider(3);
        observer = new ExampleRetryObserver();
        try {
            Retrier.execute(retryConfig, unProvider, observer);
        }
        catch (Exception e) {
            System.out.println("This was expected, it is \"urecoverable\" -> " + e);
        }
        System.out.println("  Expecting 4 action executions: " + unProvider.executions);
        System.out.println("  Expecting 0 failuresLeft: " + unProvider.failuresLeft);
        System.out.println("  Expecting to observe 4 exceptions: " + observer.exceptions);
    }

    static class ExampleActionProvider implements RetryAction<Boolean> {
        public int failuresLeft;
        public int executions;

        public ExampleActionProvider(int failuresBeforeWorking) {
            this.failuresLeft = failuresBeforeWorking + 1;
        }

        @Override
        public Boolean execute() throws Exception {
            executions++;
            if (--failuresLeft > 0) {
                throw new Exception(RECOVERABLE);
            }
            return true;
        }
    }

    static class ExampleUnrecoverableActionProvider implements RetryAction<Boolean> {
        public int failuresLeft;
        public int executions;

        public ExampleUnrecoverableActionProvider(int failuresBeforeUnrecoverable) {
            this.failuresLeft = failuresBeforeUnrecoverable + 1;
        }

        @Override
        public Boolean execute() throws Exception {
            executions++;
            if (--failuresLeft > 0) {
                throw new Exception(RECOVERABLE);
            }
            throw new Exception(UNRECOVERABLE);
        }
    }

    static class ExampleRetryObserver implements RetryObserver {
        public int exceptions = 0;

        @Override
        public boolean shouldRetry(Exception e) {
            exceptions++;
            return !e.getMessage().contains(UNRECOVERABLE);
        }
    }
}
