package io.synadia.sm;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;

/**
 * Class to make setting a per message ttl easier.
 */
public abstract class ScheduledStreamUtil {

    public static StreamInfo createSchedulableStream(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects) throws JetStreamApiException, IOException {
        StreamConfiguration sc = StreamConfiguration.builder()
            .name(streamName)
            .storageType(storageType)
            .subjects(subjects)
            .allowMessageSchedules()
            .allowMessageTtl()
            .build();
        return jsm.addStream(sc);
    }

    public static StreamInfo createSchedulableStream(JetStreamManagement jsm, StreamConfiguration startingStreamConfig) throws JetStreamApiException, IOException {
        StreamConfiguration sc = StreamConfiguration.builder(startingStreamConfig)
            .allowMessageSchedules()
            .allowMessageTtl()
            .build();
        return jsm.addStream(sc);
    }
}
