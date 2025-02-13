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

    public static void print(Object... objects) {
        StringJoiner joiner = new StringJoiner(" | ");
        for (Object o: objects) {
            joiner.add(o.toString());
        }
        System.out.println(joiner);
    }

    public static void printStateThenWait(AsyncJsPublisher publisher, ExamplePublishListener publishListener) throws InterruptedException {
        printState(publisher, publishListener);
        Thread.sleep(100);
    }

    public static void printState(AsyncJsPublisher publisher, ExamplePublishListener publishListener) {
        print(
            "elapsed=" + publishListener.elapsed(),
            "pre-flight=" + publisher.preFlightSize(),
            "in-flight=" + publisher.inFlightSize(),
            "published=" + publishListener.published,
            "acked=" + publishListener.acked,
            "exceptioned=" + publishListener.exceptioned,
            "timed out=" + publishListener.timedOut);
    }
}
