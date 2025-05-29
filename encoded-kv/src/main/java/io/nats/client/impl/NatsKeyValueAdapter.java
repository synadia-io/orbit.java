package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class NatsKeyValueAdapter extends NatsKeyValue {

    public NatsKeyValueAdapter(Connection connection, String bucketName, KeyValueOptions kvo) throws IOException {
        super((NatsConnection) connection, bucketName, kvo);
    }

    public String readSubject(String key) {
        return super.readSubject(key);
    }

    public String writeSubject(String key) {
        return super.writeSubject(key);
    }

    public void visitSubject(List<String> subjects, DeliverPolicy deliverPolicy, boolean headersOnly, boolean ordered, MessageHandler handler) throws IOException, JetStreamApiException, InterruptedException {
        ConsumerConfiguration.Builder ccb = ConsumerConfiguration.builder()
            .ackPolicy(AckPolicy.None)
            .deliverPolicy(deliverPolicy)
            .headersOnly(headersOnly)
            .filterSubjects(subjects);

        PushSubscribeOptions pso = PushSubscribeOptions.builder()
            .stream(getStreamName())
            .ordered(ordered)
            .configuration(ccb.build())
            .build();

        Duration timeout = js.getTimeout();
        JetStreamSubscription sub = js.subscribe(null, pso);
        try {
            long pending = sub.getConsumerInfo().getCalculatedPending();
            while (pending > 0) { // no need to loop if nothing pending
                Message m = sub.nextMessage(timeout);
                if (m == null) {
                    return; // if there are no messages by the timeout, we are done.
                }
                handler.onMessage(m);
                if (--pending == 0) {
                    return;
                }
            }
        }
        finally {
            sub.unsubscribe();
        }
    }
}
