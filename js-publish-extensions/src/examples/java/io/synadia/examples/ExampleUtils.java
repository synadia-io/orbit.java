// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.jnats.extension.AsyncJsPublisher;

import java.io.IOException;
import java.util.StringJoiner;

public class ExampleUtils {

    public static void setupStream(Connection nc, String stream, String subject) {
        try {
            nc.jetStreamManagement().deleteStream(stream);
        }
        catch (Exception ignore) {}
        try {
            System.out.println("Creating Stream @ " + System.currentTimeMillis());
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects(subject)
                .storageType(StorageType.File)
                .build());
        }
        catch (IOException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    public static void print(String label, Object... objects) {
        StringJoiner joiner = new StringJoiner(" | ");
        for (Object o: objects) {
            joiner.add(o.toString());
        }
        System.out.println(label + ": " + joiner);
    }

    public static void printStatus(AsyncJsPublisher publisher, ExamplePublishListener listener, boolean stopped) {
        print("Status",
            pad(stopped ? "Stopped" : (listener.paused.get() ? "Paused" : "Active"), 7),
            "pre-flight: " + pad(publisher.preFlightSize(), 8),
            "in-flight: " + pad(publisher.currentInFlight(), 8),
            "published/acked: " + pad(listener.publishedCount + "/" + listener.ackedCount, 17),
            "paused/resumed: " + pad(listener.pausedCount + "/" + listener.resumedCount, 7),
            "exceptioned/timed-out: " + pad(listener.exceptionedCount + "/" + listener.timedOutCount, 7),
            "elapsed: " + listener.elapsed() + "ms"
            );
    }

    private static final String PADDING = "                    ";
    private static String pad(Object s, int len) {
        return (s + PADDING).substring(0, len);
    }
}
