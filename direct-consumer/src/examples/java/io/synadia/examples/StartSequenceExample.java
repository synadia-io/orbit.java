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

import static io.synadia.examples.Utils.STREAM;
import static io.synadia.examples.Utils.SUBJECT;

public class StartSequenceExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Utils.setupAllowDirectStream(jsm);

            Utils.publishTestMessages(js, 100);

            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(5)
                .startSequence(25)
                .build();

            System.out.println("Expecting the first message to be received is sequence 25...");
            CountDownLatch latch = new CountDownLatch(10); // same as batch
            CompletableFuture<Boolean> f =
                dc.consume(nc.getOptions().getExecutor(), mi -> {
                    System.out.println("Received message with sequence: " + mi.getSeq());
                    latch.countDown();
                });

            // read the messages until we reach our consume goal
            latch.await(20, TimeUnit.SECONDS);

            // stop the consume
            dc.stopConsuming();

            // the consume doesn't stop immediately, there could have been a request in progress.
            f.get(5, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
