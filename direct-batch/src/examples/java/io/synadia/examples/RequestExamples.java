package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.direct.DirectBatch;
import io.synadia.direct.MessageBatchGetRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RequestExamples {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = unique();
            String subject = unique();

            // Create the stream with the allow direct flag set to true
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject)
                .allowDirect(true)
                .build();
            jsm.addStream(sc);

            // the DirectBatch can be reused on the same stream
            DirectBatch db = new DirectBatch(nc, stream);

            // There have been no messages published to the stream,
            // so the requests only return a MessageInfo with a status code 404
            System.out.println("1. When there are no messages you get a status 404");
            List<MessageInfo> list = new ArrayList<>();
            MessageBatchGetRequest request = MessageBatchGetRequest.batch(subject, 3);
            db.requestMessageBatch(request, list::add);
            printMessageInfo(list);

            // Publish a message
            js.publish(subject, "Message 1".getBytes());

            System.out.println("\n2. When there subject in the request is not found you get a status 404");
            list.clear();
            request = MessageBatchGetRequest.batch("not-a-subject", 3);
            db.requestMessageBatch(request, list::add);
            printMessageInfo(list);

            System.out.println("\n3. When there are less than the request number of messages, you receive an EOB");
            list.clear();
            request = MessageBatchGetRequest.batch(subject, 3);
            db.requestMessageBatch(request, list::add);
            printMessageInfo(list);

            js.publish(subject, "Message 2".getBytes());
            js.publish(subject, "Message 3".getBytes());
            js.publish(subject, "Message 4".getBytes());

            System.out.println("\n4. When there are enough messages, you also receive an EOB");
            list.clear();
            request = MessageBatchGetRequest.batch(subject, 3);
            db.requestMessageBatch(request, list::add);
            printMessageInfo(list);

        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printMessageInfo(List<MessageInfo> list) {
        for (int i = 0; i < list.size(); i++) {
            MessageInfo mi = list.get(i);
            if (mi.isMessage()) {
                System.out.println("[MI " + i + "] MI Message"
                    + " | subject: " + mi.getSubject()
                    + " | sequence: " + mi.getSeq());
            }
            else {
                if (mi.isEobStatus()) {
                    System.out.print("[MI " + i + "] EOB");
                }
                else if (mi.isErrorStatus()) {
                    System.out.print("[MI " + i + "] MI Error");
                }
                else if (mi.isErrorStatus()) {
                    System.out.print("[MI " + i + "] MI Status");
                }
                System.out.println(" | isStatus? " + mi.isStatus()
                    + " | isEobStatus? " + mi.isEobStatus()
                    + " | isErrorStatus? " + mi.isErrorStatus()
                    + " | status code: " + mi.getStatus().getCode());
            }
        }
    }

    private static String unique() {
        return io.nats.client.NUID.nextGlobalSequence();
    }
}
