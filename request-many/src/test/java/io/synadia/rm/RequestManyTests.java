package io.synadia.rm;

import io.nats.client.*;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static io.synadia.rm.RequestMany.DEFAULT_TOTAL_WAIT_TIME_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManyTests {

    private static RequestMany maxResponseRequest(Connection nc) {
        return RequestMany.builder(nc).maxResponses(3).build();
    }

    @Test
    public void testMaxResponseFetch() throws Exception {
        try (Connection nc = connect()) {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                List<Message> list = rm.fetch(replier.subject, null);
                assertEquals(3, list.size());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testMaxResponseIterate() throws Exception {
        try (Connection nc = connect()) {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                LinkedBlockingQueue<Message> it = rm.iterate(replier.subject, null);
                int count = 0;
                Message m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null && m != RequestMany.EOD) {
                    count++;
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }
                assertEquals(3, count);
                assertTrue(replier.latch.await(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
            }
        }
    }

    @Test
    public void testMaxResponseConsume() throws Exception {
        try (Connection nc = connect()) {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                TestRmConsumer tmc = new TestRmConsumer();
                rm.consume(replier.subject, null, tmc);
                assertTrue(tmc.eodReceived.await(3, TimeUnit.SECONDS));
                assertEquals(3, tmc.msgReceived.get());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
             }
        }
    }

    private static RequestMany maxWaitTimeRequest(Connection nc) {
        return RequestMany.builder(nc).build();
    }

    private static RequestMany maxWaitTimeRequest(Connection nc, long totalWaitTime) {
        return RequestMany.builder(nc).totalWaitTime(totalWaitTime).build();
    }

    @Test
    public void testMaxWaitTimeFetch() throws Exception {
        try (Connection nc = connect()) {
            _testMaxWaitTimeFetch(nc, DEFAULT_TOTAL_WAIT_TIME_MS);
            _testMaxWaitTimeFetch(nc, 500);
        }
    }

    private static void _testMaxWaitTimeFetch(Connection nc, long wait) throws Exception {
        try (Replier replier = new Replier(nc, 1, wait + 200, 1)) {
            RequestMany rm = maxWaitTimeRequest(nc, wait);

            long start = System.currentTimeMillis();
            List<Message> list = rm.fetch(replier.subject, null);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed > wait);
            assertEquals(1, list.size());
            assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testMaxWaitTimeIterate() throws Exception {
        try (Connection nc = connect()) {
            try (Replier replier = new Replier(nc, 1, 1200, 1)) {
                RequestMany rm = maxWaitTimeRequest(nc);

                LinkedBlockingQueue<Message> it = rm.iterate(replier.subject, null);
                int received = 0;
                Message m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null && m != RequestMany.EOD) {
                    received++;
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }

                assertEquals(1, received);
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testMaxWaitTimeConsume() throws Exception {
        try (Connection nc = connect()) {
            try (Replier replier = new Replier(nc, 1, 1200, 1)) {
                RequestMany rm = maxWaitTimeRequest(nc);

                TestRmConsumer tmc = new TestRmConsumer();
                long start = System.currentTimeMillis();
                rm.consume(replier.subject, null, tmc);
                assertTrue(tmc.eodReceived.await(DEFAULT_TOTAL_WAIT_TIME_MS * 3 / 2, TimeUnit.MILLISECONDS));
                long elapsed = System.currentTimeMillis() - start;

                assertTrue(elapsed > DEFAULT_TOTAL_WAIT_TIME_MS && elapsed < (DEFAULT_TOTAL_WAIT_TIME_MS * 2));
                assertEquals(1, tmc.msgReceived.get());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Support Classes
    // ----------------------------------------------------------------------------------------------------
    static class TestRmConsumer implements RmConsumer {
        public final CountDownLatch eodReceived = new CountDownLatch(1);
        public final AtomicInteger msgReceived = new AtomicInteger();

        @Override
        public boolean consume(Message m) {
            if (m == RequestMany.EOD) {
                eodReceived.countDown();
            }
            else {
                msgReceived.incrementAndGet();
            }
            return true;
        }
    }

    static class Replier implements AutoCloseable {
        final Dispatcher dispatcher;
        public final String subject;
        public final CountDownLatch latch;

        public Replier(final Connection nc, final int count) {
            this(nc, count, -1, -1);
        }

        public Replier(final Connection nc, final int count, final long pause, final int count2) {
            this.subject = NUID.nextGlobalSequence();
            latch = new CountDownLatch(count + (pause > 0 ? count2 : 0));

            dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < count; x++) {
                    nc.publish(m.getReplyTo(), null);
                    latch.countDown();
                }
                if (pause > 0) {
                    sleep(pause);
                    for (int x = 0; x < count2; x++) {
                        nc.publish(m.getReplyTo(), null);
                        latch.countDown();
                    }
                }
            });
            dispatcher.subscribe(subject);
        }

        public void close() throws Exception {
            dispatcher.unsubscribe(subject);
        }
    }

    private static void sleep(long pause) {
        try {
            Thread.sleep(pause);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Connection / Server Runner
    // ----------------------------------------------------------------------------------------------------
    static NatsServerRunner runner;

    private static Connection connect() throws Exception {
        return Nats.connect(getOptions());
    }

    private static Options getOptions() {
        return Options.builder().server(runner.getURI()).build();
    }

    @BeforeAll
    public static void beforeAll() {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
        try {
            runner = NatsServerRunner.builder().build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @AfterAll
    public static void afterAll() {
        try {
            runner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
