package io.synadia.examples;

import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;
import java.util.List;

class Utils {

    public static String STREAM = "dc-stream";
    public static String SUBJECT = "dc-subject";

    public static void setupAllowDirectStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        // SET UP STREAM. allowDirect must be true
        try { jsm.deleteStream(STREAM); } catch (JetStreamApiException ignore) {}
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(STREAM)
            .subjects(SUBJECT)
            .storageType(StorageType.Memory)
            .allowDirect(true)
            .build();
        jsm.addStream(sc);
    }

    public static void setupDontAllowDirectStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        try { jsm.deleteStream(STREAM); } catch (JetStreamApiException ignore) {}
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(STREAM)
            .subjects(SUBJECT)
            .storageType(StorageType.Memory)
            .build();
        jsm.addStream(sc);
    }

    public static ContinuousPublisher startContinuousPublisher(JetStream js) {
        ContinuousPublisher p = new ContinuousPublisher(js, SUBJECT);
        new Thread(p).start();
        return p;
    }

    public static void publishTestMessages(JetStream js, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(SUBJECT, null);
        }
    }

    public static void publishTestMessages(JetStream js, int count, long delay) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(SUBJECT, null);
            sleep(delay);
        }
    }

    public static void printResults(List<MessageInfo> list) {
        int s = list.size();
        if (s == 1 && list.get(0).isStatus()) {
            System.out.println("Error: " + list.get(0));
        }
        else {
            String fs = "" + (s > 0 ? list.get(0).getSeq() : "N/A");
            String ls = "" + (s > 0 ? list.get(s - 1).getSeq() : "N/A");
            System.out.println("Messages: " + s + ", First Sequence: " + fs + ", Last Sequence: " + ls);
        }
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
