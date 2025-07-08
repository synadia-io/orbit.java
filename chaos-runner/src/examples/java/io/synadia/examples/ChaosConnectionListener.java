// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;

import static io.synadia.chaos.ChaosUtils.report;

public class ChaosConnectionListener implements ConnectionListener {
    private final String reportLabel;

    public ChaosConnectionListener(String connectionName) {
        this.reportLabel = "CL/" + connectionName;
    }

    @Override
    public void connectionEvent(Connection conn, Events type) {
        report(reportLabel, type);
    }
}
