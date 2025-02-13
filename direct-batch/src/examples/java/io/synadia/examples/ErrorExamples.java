package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.direct.DirectBatchContext;
import io.synadia.direct.MessageBatchGetRequest;

import java.io.IOException;
import java.util.List;

public class ErrorExamples {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            String stream = unique();
            String subject = unique();
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject)
                .build();
            jsm.addStream(sc);

            System.out.println("1. Stream must have allow direct set.");
            // 1A. Stream must have allow direct set
            try {
                new DirectBatchContext(nc, stream);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  A. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }
            jsm.deleteStream(stream); // clean up the stream for the next part

            // 1B. Create the stream with the allow direct flag set to true
            sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .subjects(subject)
                .allowDirect(true)
                .build();
            jsm.addStream(sc);

            DirectBatchContext db = new DirectBatchContext(nc, stream);
            System.out.println("  B. DirectBatchContext created: " + db);

            System.out.println("\n2. When creating a DirectBatchContext object...");
            // 2.A Connection required, cannot be null
            try {
                new DirectBatchContext(null, stream);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  A. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }

            // 2B/2C. Stream name required, cannot be null or empty
            try {
                new DirectBatchContext(nc, null);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  B. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }
            try {
                new DirectBatchContext(nc, "");
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  C. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }

            System.out.println("\n3. When creating a MessageBatchGetRequest object...");
            // 3A. Subject cannot be null or empty
            try {
                MessageBatchGetRequest.batch(null, 1);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  A. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }

            // 3B. MessageBatchGetRequest... Subject cannot be null or empty
            try {
                MessageBatchGetRequest.batch("", 1);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  B. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }

            // 3C. MessageBatchGetRequest... Batch must be greater than zero
            try {
                MessageBatchGetRequest.batch(">", 0);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  C. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }

            // 3D. MessageBatchGetRequest... Subjects are required.
            try {
                List<String> subjects = null;
                MessageBatchGetRequest.multiLastForSubjects(subjects);
            }
            catch (IllegalArgumentException iae) {
                System.out.println("  D. Expected! IllegalArgumentException '" + iae.getMessage() + "'");
            }
        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            throw new RuntimeException(e);
        }
    }

    private static String unique() {
        return io.nats.client.NUID.nextGlobalSequence();
    }
}
