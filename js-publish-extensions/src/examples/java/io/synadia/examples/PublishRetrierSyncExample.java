// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.jnats.extension.PublishRetrier;
import io.synadia.jnats.extension.PublishRetryConfig;
import io.synadia.jnats.extension.RetryCondition;

import java.time.Duration;

/**
 * Publish sync retried example
 */
public class PublishRetrierSyncExample {

    public static String STREAM = "pr-sync-stream";
    public static String SUBJECT = "pr-sync-subject";

    public static void main(String[] args) {
        Options options = Options.builder()
            .connectionListener((x,y) -> {})
            .errorListener(new ErrorListener() {})
            .build();
        try (Connection nc = Nats.connect(options)) {
            // create the stream, delete any existing one first for example purposes.
            try { nc.jetStreamManagement().deleteStream(STREAM); }  catch (Exception ignore) {}
            System.out.println("Creating Stream @ " + System.currentTimeMillis());
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .storageType(StorageType.Memory)
                .build());

            // --------------------------------------------------------------------------------
            // PublishRetryConfig...
            // default attempts is 2
            // default backoff is {250, 250, 500, 500, 3000, 5000}
            // default deadline is unlimited
            // default Retry Conditions are timeouts and no responders (both IOExceptions)
            //   and shown here for example
            // --------------------------------------------------------------------------------
            // This config will retry 3 times with a wait of 500 millis between retries
            // but since the deadline is short, the deadline will short circuit that.
            // This should be tuned to match your needs.
            // --------------------------------------------------------------------------------
            PublishRetryConfig config = PublishRetryConfig.builder()
                .attempts(3)
                .backoffPolicy(new long[]{500})
                .deadline(Duration.ofSeconds(2))
                .retryConditions(RetryCondition.NoResponders, RetryCondition.IoEx)
                .build();

            int num = 0;
            boolean keepGoing;
            do {
                long now = System.currentTimeMillis();
                System.out.print("Publishing @ " + (++num) + "...");
                PublishAck pa = PublishRetrier.publish(config, nc.jetStream(), SUBJECT, null);
                long elapsed = System.currentTimeMillis() - now;
                keepGoing = false;
                if (pa == null) {
                    System.out.println("No Publish Ack after " + elapsed);
                }
                else if (pa.hasError()) {
                    System.out.println("Publish Ack after " + elapsed + " but got error: " + pa.getError());
                }
                else {
                    keepGoing = true;
                    System.out.println("Publish Ack after " + elapsed + " --> " + pa.getJv().toJson());
                }
            } while (keepGoing);
        }
        catch (Exception e) {
            System.out.println("Probably can't connect... " + e);
        }
    }
}
