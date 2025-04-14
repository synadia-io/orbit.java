// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ErrorListenerConsoleImpl;

public class TransactionalPublishExample {

    public static final String STREAM = "exampleStream";
    public static final String SUBJECT = "exampleSubject";

    public static void main(String[] args) {
        Options options = Options.builder()
            .server(Options.DEFAULT_URL)
            .connectionListener((connection, events) -> ExampleUtils.print("Connection Event:" + events.getEvent()))
            .errorListener(new ErrorListenerConsoleImpl())
            .build();

        try (Connection nc = Nats.connect(options)) {
            ExampleUtils.setupStream(nc, STREAM, SUBJECT);

            
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }
}
