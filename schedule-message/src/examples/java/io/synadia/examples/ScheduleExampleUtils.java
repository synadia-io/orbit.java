// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.Debug;

import java.io.IOException;

public class ScheduleExampleUtils {

    public static void createOrReplaceStream(Connection connection, String stream, String... subjects) throws IOException, JetStreamApiException {
        createOrReplaceStream(connection.jetStreamManagement(), stream, subjects);
    }

    public static void createOrReplaceStream(JetStreamManagement jsm, String stream, String... subjects) throws IOException, JetStreamApiException {
        report("createOrReplaceStream");
        try {
            jsm.deleteStream(stream);
        }
        catch (Exception ignore) {}

        try {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subjects)
                .allowMessageSchedules()
                .build();
            StreamInfo si = jsm.addStream(sc);
            Debug.info("Created stream: " + si.getConfiguration());
        }
        catch (Exception e) {
            Debug.info("Failed creating stream: '' " + e);
            System.exit(-1);
        }
    }

    public static void report(String... strings) {
        System.out.println("[" + System.currentTimeMillis() + "] " + String.join(" ", strings));
    }
}
