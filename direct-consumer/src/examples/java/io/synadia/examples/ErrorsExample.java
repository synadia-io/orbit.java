package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.synadia.dc.DirectConsumer;
import io.synadia.dc.DirectConsumerBuilder;

import java.util.concurrent.CompletableFuture;

import static io.synadia.examples.Utils.*;

public class ErrorsExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            setupDontAllowDirectStream(jsm);

            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(10)
                .build();

            System.out.println("No Allow Direct on stream.");
            Utils.printResults(dc.fetch());

            Utils.setupAllowDirectStream(jsm);

            System.out.println("\nInvalid Subject: Empty Or Null");
            dc = new DirectConsumerBuilder(jsm, STREAM, "")
                .batch(10)
                .build();
            Utils.printResults(dc.fetch());

            // multiple parallel requests
            System.out.println("\nInvalid Call:");
            dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(10)
                .build();
            CompletableFuture<Boolean> f = dc.consume(nc.getOptions().getExecutor(), mi -> {});
            try {
                dc.next();
            }
            catch (Exception e) {
                System.out.println(e);
            }
//            dc.stopConsuming();
            f.cancel(true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
