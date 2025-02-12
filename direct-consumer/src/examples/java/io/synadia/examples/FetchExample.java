package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.synadia.direct.DirectConsumer;
import io.synadia.direct.DirectConsumerBuilder;

import static io.synadia.examples.Utils.*;

public class FetchExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            setupAllowDirectStream(jsm);

            publishTestMessages(js, 25);

            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(10)
                .build();

            printResults(dc.fetch());
            printResults(dc.fetch());
            printResults(dc.fetch());
            printResults(dc.fetch());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
