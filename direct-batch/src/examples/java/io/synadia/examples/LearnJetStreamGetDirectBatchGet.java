// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamConfiguration;
import io.synadia.direct.DirectBatchContext;
import io.synadia.direct.MessageBatchGetRequest;

import java.util.List;

public class LearnJetStreamGetDirectBatchGet {
    static final String NATS_URL = System.getenv("NATS_URL") != null
        ? System.getenv("NATS_URL") : "nats://localhost:4222";
    static final String STREAM = "ORDERS";
    static final String SUBJECTS = "orders.>";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Ensure an ORDERS stream exists with direct access enabled,
            // then seed a few orders so the batch get has something to read.
            try { jsm.deleteStream(STREAM); } catch (JetStreamApiException ignore) {}
            jsm.addStream(StreamConfiguration.builder()
                .name(STREAM)
                .subjects(SUBJECTS)
                .allowDirect(true)
                .build());

            js.publish("orders.created", "{\"id\":\"order-1\"}".getBytes());
            js.publish("orders.created", "{\"id\":\"order-2\"}".getBytes());
            js.publish("orders.created", "{\"id\":\"order-3\"}".getBytes());

            // NATS-DOC-START
            // Batch Direct Get: in one request, read up to 3 messages from the
            // ORDERS stream starting at stream sequence 1, then iterate in order.
            DirectBatchContext direct = new DirectBatchContext(nc, STREAM);
            MessageBatchGetRequest request = MessageBatchGetRequest.batch(">", 3, 1);

            List<MessageInfo> messages = direct.fetchMessageBatch(request);
            for (MessageInfo mi : messages) {
                System.out.println("sequence " + mi.getSeq() + " | subject " + mi.getSubject());
            }
            // NATS-DOC-END
        }
    }
}
