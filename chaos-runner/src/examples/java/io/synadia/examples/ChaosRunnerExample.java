// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.chaos.ChaosArguments;
import io.synadia.chaos.ChaosRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static io.synadia.chaos.ChaosUtils.report;

public class ChaosRunnerExample {
    private static final int SERVER_COUNT = 3; // 1, 3, 5
    private static final long DELAY = 5000; // the delay to bring a server down
    private static final long INITIAL_DELAY = 10000; // the delay to bring a server down the first time
    private static final long DOWN_TIME = 5000; // how long before bringing the server up
    private static final int HEALTH_CHECK_DELAY = 3000;

    private static final int NUM_CONNECTIONS = 5;

    public static void main(String[] args) throws Exception {
        ChaosArguments arguments = new ChaosArguments()
            .servers(SERVER_COUNT)
            .workDirectory("C:\\temp\\chaos-runner")
            .serverNamePrefix("cr-example-server")
            .clusterName("cr-example-cluster")
            .delay(DELAY)
            .initialDelay(INITIAL_DELAY)
            .downTime(DOWN_TIME)
            // this is done last so anything on the command line
            // is used over the hard coded items.
            .args(args);

        ChaosRunner runner = ChaosRunner.start(arguments);

        // just give the servers a little time to be ready be first connect
        Thread.sleep(1000);

        String[] urls = runner.getConnectionUrls();
        report("Connection Urls");
        for (String url : urls) {
            report(" ", url);
        }

        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        List<Connection> connections = new ArrayList<>(urls.length);
        for (int i = 0; i < NUM_CONNECTIONS; i++) {
            String connectionName = "Conn" + (i + 1);
            Options options = Options.builder().servers(urls)
                .connectionListener(new ChaosConnectionListener(connectionName))
                .errorListener(new ChaosErrorListener(connectionName))
                .build();

            Connection connection = Nats.connect(options);
            connections.add(connection);
        }

        int[] ports = runner.getConnectionPorts();
        int[] monitorPorts = runner.getMonitorPorts();
        boolean hasMonitor = monitorPorts[0] > 0;

        String[] reports = new String[ports.length];
        while (true) {
            Thread.sleep(HEALTH_CHECK_DELAY);
            if (hasMonitor) {
                report("HealthZ");
                for (int i = 0; i < monitorPorts.length; i++) {
                    int port = ports[i];
                    int mport = monitorPorts[i];
                    report(" ", port + "/" + mport, readHealthz(mport));
                }
            }
        }
    }

    private static String readHealthz(int port) {
        return readEndpoint(port, "healthz");
    }

    private static String readEndpoint(int port, String endpoint) {
        String sUrl = "http://localhost:" + port + "/" + endpoint;
        try {
            URL url = new URL(sUrl);
            InputStream inputStream = url.openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            boolean first = true;
            String line;
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                if (first) {
                    first = false;
                }
                else {
                    content.append(System.lineSeparator());
                }
                content.append(line);
            }
            reader.close();
            return content.toString().trim();
        }
        catch (IOException e) {
            return e.getMessage();
        }
    }
}
