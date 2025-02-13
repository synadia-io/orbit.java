package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.direct.DirectBatchContext;
import io.synadia.direct.MessageBatchGetRequest;

import java.io.IOException;
import java.util.List;

public class FetchMessageBatchExamples {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = ExampleUtils.appendRandomString("rmb-stream-");
            String subject = ExampleUtils.appendRandomString("rmb-subject-");

            // Create the stream with the allow direct flag set to true
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject)
                .allowDirect(true)
                .build();
            jsm.addStream(sc);

            // the DirectBatchContext can be reused on the same stream
            DirectBatchContext context = new DirectBatchContext(nc, stream);

            // There have been no messages published to the stream,
            // so the requests only return a MessageInfo with a status code 404
            System.out.println("1. When there are no messages you get a status 404");
            MessageBatchGetRequest mbgr = MessageBatchGetRequest.batch(subject, 3);
            List<MessageInfo> list = context.fetchMessageBatch(mbgr);
            ExampleUtils.printMessageInfo(list);

            // Publish a message
            js.publish(subject, "Message 0".getBytes());

            System.out.println("\n2. When the subject in the request is not found you get a status 404");
            mbgr = MessageBatchGetRequest.batch("not-a-subject", 3);
            list = context.fetchMessageBatch(mbgr);
            ExampleUtils.printMessageInfo(list);

            System.out.println("\n3. When there are less than the request number of messages, you just get what's available.");
            mbgr = MessageBatchGetRequest.batch(subject, 3);
            list = context.fetchMessageBatch(mbgr);
            ExampleUtils.printMessageInfo(list);

            // print some more messages
            for (int x = 1; x < 10; x++) {
                js.publish(subject, ("Message " + x).getBytes());
            }

            System.out.println("\n4. When there are enough messages, you get the number of messages in the batch size.");
            mbgr = MessageBatchGetRequest.batch(subject, 3);
            list = context.fetchMessageBatch(mbgr);
            ExampleUtils.printMessageInfo(list);

        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }
}
