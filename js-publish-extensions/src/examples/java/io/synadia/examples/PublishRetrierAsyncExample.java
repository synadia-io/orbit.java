// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.jnats.extension.PublishRetrier;
import io.synadia.jnats.extension.PublishRetryConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class PublishRetrierAsyncExample {

    public static String STREAM = "pr-async-stream";
    public static String SUBJECT = "pr-async-subject";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {
            // create the stream, delete any existing one first for example purposes.
            try { nc.jetStreamManagement().deleteStream(STREAM); }  catch (Exception ignore) {}
            System.out.println("Creating Stream @ " + System.currentTimeMillis());
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .storageType(StorageType.File) // so it's persistent for a server restart test
                .build());

            // default attempts is 2, we change this so you have time to kill the server to test.
            // the default backoff is {250, 250, 500, 500, 3000, 5000}
            // the default deadline is 1 hour
            // the default Retry Conditions are timeouts and no responders (both IOExceptions)
            PublishRetryConfig config = PublishRetryConfig.builder().attempts(10).build();

            int num = 0;
            while (true) {
                long now = System.currentTimeMillis();
                System.out.println("Publishing @ " + (++num));
                CompletableFuture<PublishAck> cfpa = PublishRetrier.publishAsync(config, nc.jetStream(), SUBJECT, ("data" + num).getBytes());
                PublishAck pa = cfpa.get(30, TimeUnit.SECONDS);
                long elapsed = System.currentTimeMillis() - now;
                System.out.println("Publish Ack after " + elapsed + " --> " + pa.getJv().toJson());
            }
        }
        catch (Exception e) {
            System.out.println("EXAMPLE EXCEPTION: " + e);
            e.printStackTrace();
        }
    }
}
