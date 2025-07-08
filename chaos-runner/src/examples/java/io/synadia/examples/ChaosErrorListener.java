// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.impl.ErrorListenerConsoleImpl;
import io.nats.client.support.Status;

import static io.synadia.chaos.ChaosUtils.report;

public class ChaosErrorListener extends ErrorListenerConsoleImpl {
    private final String reportLabel;

    public ChaosErrorListener(String connectionName) {
        this.reportLabel = "EL/" + connectionName;
    }

    @Override
    public void errorOccurred(Connection conn, String error) {
        report(reportLabel, supplyMessage("[SEVERE] errorOccurred", conn, null, null, "Error: ", error));
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        report(reportLabel, supplyMessage("[SEVERE] exceptionOccurred", conn, null, null, "Exception: ", exp));
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
    }

    @Override
    public void messageDiscarded(Connection conn, Message msg) {
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
        report(reportLabel, supplyMessage("[SEVERE] heartbeatAlarm", conn, null, sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence));
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
    }

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
    }

    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
    }

    @Override
    public void socketWriteTimeout(Connection conn) {
    }
}
