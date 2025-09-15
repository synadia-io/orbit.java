// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.*;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Debug;
import io.synadia.sm.ScheduledMessageBuilder;

import java.util.concurrent.CountDownLatch;

public class ScheduleAtSpecificTime {
    public static final String STREAM = "scheduler-stream";
    public static final String SCHEDULER_SUBJECT = "scheduler-subject";
    public static final String TARGET_SUBJECT = "target-subject";

    public static void main(String[] args) {
        try {
            Options options = new Options.Builder()
                .server("nats://localhost:4222")
                .errorListener(new ErrorListener() {})
                .build();

            try (Connection connection = Nats.connectReconnectOnConnect(options)) {
                ScheduleExampleUtils.createOrReplaceStream(connection, STREAM, SCHEDULER_SUBJECT, TARGET_SUBJECT);
                JetStream js = connection.jetStream();

                CountDownLatch latch = new CountDownLatch(2);
                Dispatcher d = connection.createDispatcher();

                // subscribe to the subject that receives the schedule message
                js.subscribe(SCHEDULER_SUBJECT, d, m -> {
                    Debug.info("SCHEDULE", m);
                    m.ack();
                }, false);

                // subscribe to the target subject
                js.subscribe(TARGET_SUBJECT, d, m -> {
                    Debug.info("TARGET", m);
                    m.ack();
                    latch.countDown();
                }, false);

                Message m = new ScheduledMessageBuilder()
                    .publishSubject(SCHEDULER_SUBJECT)
                    .targetSubject(TARGET_SUBJECT)
                    .scheduleAt(DateTimeUtils.gmtNow().plusSeconds(3))
                    .data("payload")
                    .build();
                Debug.info("PUBLISH", m);
                js.publish(m);

                latch.await();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
