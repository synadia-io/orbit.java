package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.MessageInfo;
import io.synadia.dc.DirectConsumer;
import io.synadia.dc.DirectConsumerBuilder;

import java.time.ZonedDateTime;

import static io.synadia.examples.Utils.*;

public class StartTimeExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Utils.setupAllowDirectStream(jsm);

            System.out.println("Publishing will take 6 seconds...");
            Utils.publishTestMessages(js, 3, 2000);

            MessageInfo mi = jsm.getMessage(STREAM, 1);
            ZonedDateTime zdt = mi.getTime().plusSeconds(1);

            System.out.println("Expecting the first message to be received is sequence 2...");
            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(10)
                .startTime(zdt)
                .build();

            printResults(dc.fetch());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
