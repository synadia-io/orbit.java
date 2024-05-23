package io.synadia.jnats.extension;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.jnats.extension.RetryConfig.*;
import static org.junit.jupiter.api.Assertions.*;

public class RetrierTests {
    @Test
    public void testRetryConfigBuilding() {
        RetryConfig rc = DEFAULT_CONFIG;
        assertEquals(DEFAULT_ATTEMPTS, rc.getAttempts());
        assertArrayEquals(DEFAULT_BACKOFF_POLICY, rc.getBackoffPolicy());
        assertEquals(Long.MAX_VALUE, rc.getDeadline());

        long[] backoffPolicy = new long[]{300};
        rc = RetryConfig.builder()
            .attempts(1)
            .backoffPolicy(backoffPolicy)
            .deadline(1000)
            .build();

        assertEquals(1, rc.getAttempts());
        assertArrayEquals(backoffPolicy, rc.getBackoffPolicy());
        assertEquals(1000, rc.getDeadline());

        rc = RetryConfig.builder()
            .attempts(0)
            .deadline(0)
            .build();

        assertEquals(DEFAULT_ATTEMPTS, rc.getAttempts());
        assertEquals(Long.MAX_VALUE, rc.getDeadline());
    }

    @Test
    public void testRetryExecute() {
        AtomicInteger counterExhaustAttempts = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(DEFAULT_CONFIG,
                () -> { throw new Exception("Attempt: " + counterExhaustAttempts.incrementAndGet()); }));
        assertEquals(3, counterExhaustAttempts.get());

        AtomicInteger counterExhaustDeadline = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().attempts(Integer.MAX_VALUE).deadline(500).build(),
                () -> { throw new Exception("Attempt: " + counterExhaustDeadline.incrementAndGet()); }));
        assertTrue(counterExhaustDeadline.get() > 1);

        AtomicInteger counterExhaustObserver = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().attempts(Integer.MAX_VALUE).build(),
                () -> { throw new Exception("Attempt: " + counterExhaustObserver.incrementAndGet()); },
                e -> counterExhaustObserver.get() < 5));
        assertEquals(5, counterExhaustObserver.get());

        long[] policy = new long[]{100};
        AtomicInteger counterMoreAttemptsThanPolicyEntries = new AtomicInteger();
        long start = System.currentTimeMillis();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().backoffPolicy(policy).build(),
                () -> { throw new Exception("Attempt: " + counterMoreAttemptsThanPolicyEntries.incrementAndGet());}));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 300);
    }
}
