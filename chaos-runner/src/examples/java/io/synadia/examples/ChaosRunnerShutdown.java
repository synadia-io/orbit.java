// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.synadia.chaos.ChaosArguments;
import io.synadia.chaos.ChaosRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import static io.synadia.chaos.ChaosUtils.out;

public class ChaosRunnerShutdown {

    public static void main(String[] args) throws Exception {
        ChaosArguments arguments = new ChaosArguments()
            .servers(3)
            .workDirectory("C:\\temp\\chaos-runner")
            .serverNamePrefix("cr-shutdown-server")
            .clusterName("cr-shutdown-cluster")
            .delay(30_000)
            .initialDelay(30_000)
            .downTime(30_000);

        ChaosRunner runner = ChaosRunner.start(arguments);

        // just give the servers a little time to be ready be first connect
        Thread.sleep(1000);

        String[] urls = runner.getConnectionUrls();
        out("Connection Urls");
        for (String url : urls) {
            out(" ", url);
        }

        int[] ports = runner.getConnectionPorts();
        int[] monitorPorts = runner.getMonitorPorts();

        Thread.sleep(1000);
        out("H RUNNING", ChaosRunner.isRunning());
        for (int i = 0; i < monitorPorts.length; i++) {
            int port = ports[i];
            int mport = monitorPorts[i];
            String hz = readHealthz(monitorPorts[i]);
            out("H", port + "/" + mport, hz);
        }

        ChaosRunner.shutdown();

        Thread.sleep(1000);
        out("Z RUNNING", ChaosRunner.isRunning());
        for (int i = 0; i < monitorPorts.length; i++) {
            int port = ports[i];
            int mport = monitorPorts[i];
            String hz = readHealthz(monitorPorts[i]);
            out("Z", port + "/" + mport, hz);
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
