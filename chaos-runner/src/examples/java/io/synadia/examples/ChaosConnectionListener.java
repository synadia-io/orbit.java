// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;

import java.util.concurrent.atomic.AtomicInteger;

import static io.synadia.chaos.ChaosUtils.out;

public class ChaosConnectionListener implements ConnectionListener {
    private static String message(Events event) {
        switch (event) {
            case CONNECTED: return "Connected";
            case CLOSED: return "Closed";
            case DISCONNECTED: return "Disconnected";
            case RECONNECTED: return "Re-Connected";
            case RESUBSCRIBED: return "Subscriptions Re-Established";
            case DISCOVERED_SERVERS: return "Servers Discovered";
            case LAME_DUCK: return "Entering lame duck mode";
        }
        return "";
    };

    private final String connectionName;
    private final AtomicInteger currentPort;

    public ChaosConnectionListener(String connectionName) {
        this.connectionName = connectionName;
        currentPort = new AtomicInteger(0);
    }

    @Override
    public void connectionEvent(Connection conn, Events type) {
        int cur;
        if (type == Events.CONNECTED) {
            cur = conn.getServerInfo().getPort();
            currentPort.set(cur);
        }
        else {
            cur = currentPort.get();
        }
        if (cur == 0) {
            out("CL", connectionName, message(type));
        }
        else {
            out("CL", connectionName, message(type), "Port: " + cur);
        }
    }
}
