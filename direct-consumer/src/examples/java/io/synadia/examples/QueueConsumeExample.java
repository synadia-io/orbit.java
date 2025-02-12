package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.MessageInfo;
import io.synadia.direct.DirectConsumer;
import io.synadia.direct.DirectConsumerBuilder;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.synadia.examples.Utils.*;

public class QueueConsumeExample {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Utils.setupAllowDirectStream(jsm);

            ContinuousPublisher p = startContinuousPublisher(js);

            DirectConsumer dc = new DirectConsumerBuilder(jsm, STREAM, SUBJECT)
                .batch(100)
                .build();

            LinkedBlockingQueue<MessageInfo> q = dc.queueConsume(nc.getOptions().getExecutor());
            int count = 0;
            while (count < 301) {
                MessageInfo mi = q.poll(1, TimeUnit.SECONDS);
                if (mi != null) {
                    if (++count % 100 == 0) {
                        System.out.println("In progress: " + count + " messages.");
                    }
                }
            }

            // stop the consume
            dc.stopConsuming();
            System.out.println("After 'stop' there are " + count + " messages.");

            // the consume doesn't stop immediately, there could have been a request in progress.
            MessageInfo mi = q.poll(1, TimeUnit.SECONDS);
            while (mi != null) {
                ++count;
                mi = q.poll(1, TimeUnit.SECONDS);
            }
            System.out.println("At end there are " + count + " messages.");

            p.stop(); // stop the publishing or the program won't end since it's running on a thread
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
