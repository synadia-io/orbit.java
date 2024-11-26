package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.synadia.dc.DirectConsumer;
import io.synadia.dc.DirectConsumerBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.examples.Utils.*;

public class ConsumeExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Utils.setupAllowDirectStream(jsm);

            ContinuousPublisher p = startContinuousPublisher(js);

            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(100)
                .build();

            CountDownLatch latch = new CountDownLatch(301);
            AtomicInteger count = new AtomicInteger();
            CompletableFuture<Boolean> f =
                dc.consume(nc.getOptions().getExecutor(), mi -> {
                    if (count.incrementAndGet() % 100 == 0) {
                        System.out.println("In progress: " + count.get() + " messages.");
                    }
                    latch.countDown();
                });

            // read the messages until we reach our consume goal
            latch.await(30, TimeUnit.SECONDS);

            // stop the consume
            dc.stopConsuming();
            System.out.println("After 'stop' there are " + count.get() + " messages.");

            // the consume doesn't stop immediately, there could have been a request in progress.
            f.get(5, TimeUnit.SECONDS);
            System.out.println("The future completed with " + count.get() + " messages.");

            p.stop(); // stop the publishing or the program won't end since it's running on a thread
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
