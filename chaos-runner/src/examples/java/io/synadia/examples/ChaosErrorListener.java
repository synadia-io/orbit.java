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
    private final String connectionName;

    public ChaosErrorListener(String connectionName) {
        this.connectionName = connectionName;
    }

    @Override
    public void errorOccurred(Connection conn, String error) {
        report("EL", connectionName, "Error", error);
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        report("EL", connectionName, "Exception", exp);
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
    }

    @Override
    public void messageDiscarded(Connection conn, Message msg) {
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
        report("EL", connectionName, "Heartbeat Alarm", "Last Stream Sequence: " + lastStreamSequence, "Last Consumer Sequence: " + lastConsumerSequence);
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
        report("EL", connectionName, "Socket Write Timeout");
    }
}
