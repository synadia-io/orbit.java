// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.jnats.extension.Retrier;
import io.synadia.jnats.extension.RetryConfig;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RetrierPublishAsyncExample {

    public static String STREAM = "retrier";
    public static String SUBJECT = "retriersub";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            try {
                nc.jetStreamManagement().deleteStream(STREAM);
            }
            catch (Exception ignore) {}

            // since the default backoff is {250, 250, 500, 500, 3000, 5000}
            new Thread(() -> {
                try {
                    Thread.sleep(1100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try {
                    System.out.println("Creating Stream @ " + System.currentTimeMillis());
                    nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                        .name(STREAM)
                        .subjects(SUBJECT)
                        .storageType(StorageType.Memory)
                        .build());
                }
                catch (IOException | JetStreamApiException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            RetryConfig config = RetryConfig.builder().attempts(10).build();
            long now = System.currentTimeMillis();

            System.out.println("Publishing @ " + now);
            CompletableFuture<PublishAck> cfpa = Retrier.publishAsync(config, nc.jetStream(), SUBJECT, null);
            PublishAck pa = cfpa.get(30, TimeUnit.SECONDS);
            long done = System.currentTimeMillis();

            System.out.println("Publish Ack: " + pa.getJv().toJson());
            System.out.println("Done @ " + done + ", Elapsed: " + (done - now));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
