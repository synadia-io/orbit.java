// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.ErrorListenerConsoleImpl;
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
            .server(Options.DEFAULT_URL)
            .connectionListener((connection, events) -> ExampleUtils.print("Connection Event", events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();
        try (Connection nc = Nats.connect(options)) {
            // create the stream, delete any existing one first for example purposes.
            try { nc.jetStreamManagement().deleteStream(STREAM); }  catch (Exception ignore) {}
            nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECT)
                .storageType(StorageType.File) // so it's persistent for a server restart test
                .build());

            // --------------------------------------------------------------------------------
            // PublishRetryConfig...
            // default attempts is 2
            // default backoff is {250, 250, 500, 500, 3000, 5000}
            // default deadline is unlimited
            // default Retry Conditions, shown here for example, are
            //   too many requests, timeouts and no responders
            // --------------------------------------------------------------------------------
            // This config will retry 5 times with a wait of 1000 millis between retries
            // but since the deadline is short, the deadline will short circuit that.
            // This should be tuned to match your needs. For testing purposes, try these:
            // 1. Stop and then restart the server quickly. Wait for the publishing to resume.
            // 2. Stop the server, wait for the program to end and see the retry exhausted.
            // --------------------------------------------------------------------------------
            PublishRetryConfig config = PublishRetryConfig.builder()
                .attempts(5)
                .backoffPolicy(new long[]{1000})
                .deadline(Duration.ofSeconds(10)) // more than the attempt x backoff. Here for reference.
                .retryConditions(RetryCondition.NoResponders, RetryCondition.IoEx)
                .build();

            int num = 0;
            boolean keepGoing = true;
            while (keepGoing) {
                try {
                    System.out.print("Publishing @ " + (++num) + "...");
                    PublishAck pa = PublishRetrier.publish(config, nc.jetStream(), SUBJECT, null);
                    if (pa == null) {
                        // this would be unusual.
                        System.out.println("No Publish Ack");
                    }
                    else if (pa.hasError()) {
                        // This represents the server saying it got the message but could not complete
                        // the publishing of it. Maybe the leader node is down...
                        System.out.println("Publish Ack,  but got error: " + pa.getError());
                    }
                    else {
                        // the happy path
                        System.out.println("Publish Ack --> " + pa.getJv().toJson());
                    }
                }
                catch (Exception e) {
                    // This is where you should end up when the retry cannot get publish message
                    // after it's gone through its paces. It will be the last exception it got.
                    System.out.println("Cause of the retry failure: " + e);
                    keepGoing = false;
                }
            }
        }
        catch (Exception e) {
            System.out.println("Probably can't connect... " + e);
        }
    }
}
