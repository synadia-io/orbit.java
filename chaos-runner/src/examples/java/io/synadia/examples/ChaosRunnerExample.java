// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.chaos.ChaosRunner;
import io.synadia.chaos.ChaosStarter;

import java.util.ArrayList;
import java.util.List;

import static io.synadia.chaos.ChaosUtils.report;

public class ChaosRunnerExample {
    static final int NUM_CONNECTIONS = 2;
    static final int SERVER_COUNT = 1; // try 1, 3, 5
    static final long DELAY = 5000; // the delay to bring a server down
    static final long DOWN_TIME = 5000; // how long before bringing the server up
    static final long STAY_ALIVE = 30_000; // how long to run the example program

    public static void main(String[] args) throws Exception {
        ChaosRunner runner = new ChaosStarter()
            .count(SERVER_COUNT)
            .delay(DELAY)
            .downTime(DOWN_TIME)
            .start();

        // just give the servers a little time to be ready be first connect
        Thread.sleep(1000);

        String[] urls = runner.getConnectionUrls();
        report("EXAMPLE", "Connection Urls:");
        for (String url : urls) {
            report("EXAMPLE", "  " + url);
        }

        List<Connection> connections = new ArrayList<>(urls.length);
        for (int i = 0; i < NUM_CONNECTIONS; i++) {
            String cn = "CONN/" + i;
            Options options = Options.builder().servers(urls)
                .connectionListener(new ChaosConnectionListener(cn))
                .errorListener(new ChaosErrorListener(cn))
                .build();

            Connection connection = Nats.connect(options);
            connections.add(connection);
            report("EXAMPLE", "Initial connection for " + cn, "Port: " + connection.getServerInfo().getPort());
        }

        // this just allows time for the runner to work
        Thread.sleep(STAY_ALIVE);
        System.exit(0);
    }
}
