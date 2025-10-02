// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

import io.nats.ClusterInsert;
import io.nats.ClusterNode;
import io.nats.NatsServerRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.nats.NatsRunnerUtils.*;
import static io.synadia.chaos.ChaosUtils.getDefaultPrinter;

public class ChaosRunner {

    private static final String CR_LABEL = "ChaosRunner";

    private static ChaosRunner INSTANCE;

    public final ChaosPrinter printer;
    public final int servers;
    public final String clusterName;
    public final String serverNamePrefix;
    public final boolean js;
    public final Path jsStoreDirBase;
    public final long initialDelay;
    public final long delay;
    public final long downTime;
    public final boolean random;
    public final int specificPort;
    public final int port;
    public final int listen;
    public final int monitor;

    private final List<ClusterInsert> clusterInserts;
    private final List<NatsServerRunner> natsServerRunners;
    private final ScheduledThreadPoolExecutor executor;
    private int downIx = 0;

    public int[] getConnectionPorts() {
        int[] ports = new int[servers];
        for (int ix = 0; ix < servers; ix++) {
            ports[ix] = clusterInserts.get(ix).node.port;
        }
        return ports;
    }

    public int[] getListenPorts() {
        int[] lports = new int[servers];
        for (int ix = 0; ix < servers; ix++) {
            lports[ix] = clusterInserts.get(ix).node.listen;
        }
        return lports;
    }

    public int[] getMonitorPorts() {
        int[] mports = new int[servers];
        for (int ix = 0; ix < servers; ix++) {
            Integer mport = clusterInserts.get(ix).node.monitor;
            mports[ix] = mport == null ? 0 : mport;
        }
        return mports;
    }

    public String[] getConnectionUrls() {
        String[] urls = new String[servers];
        for (int ix = 0; ix < servers; ix++) {
            urls[ix] = getNatsLocalhostUri(clusterInserts.get(ix).node.port);
        }
        return urls;
    }

    private NatsServerRunner createRunner(int index) throws Exception {
        ClusterInsert ci = clusterInserts.get(index);
        NatsServerRunner.Builder b = NatsServerRunner.builder()
            .debug(false)
            .jetstream(true)
            .configInserts(ci.configInserts)
            .port(ci.node.port)
            .connectCheckTries(0)
            ;
        return b.build();
    }

    private void scheduleDown(long delay) {
        executor.schedule(this::downTask, delay, TimeUnit.MILLISECONDS);
    }

    private void scheduleUp() {
        executor.schedule(this::upTask, downTime, TimeUnit.MILLISECONDS);
    }

    private void downTask() {
        try {
            if (specificPort != -1) {
                for (int i = 0; i < natsServerRunners.size(); i++) {
                    NatsServerRunner nsr = natsServerRunners.get(i);
                    if (nsr.getPort() == specificPort) {
                        downIx = i;
                        break;
                    }
                }
            }
            else if (random) {
                downIx = ThreadLocalRandom.current().nextInt(servers);
            }

            NatsServerRunner runner = natsServerRunners.remove(downIx);
            printer.out(CR_LABEL, "DOWN", runner.getPort());
            clusterInserts.add(clusterInserts.remove(downIx));
            runner.close();
            scheduleUp();
        }
        catch (Throwable e) {
                printer.out(CR_LABEL, "DOWN/EX", e);
        }
    }

    private void upTask() {
        try {
            NatsServerRunner runner = createRunner(servers - 1);
                printer.out(CR_LABEL, "UP", runner.getPort());
            natsServerRunners.add(runner);
            scheduleDown(delay);
        }
        catch (Throwable e) {
                printer.out(CR_LABEL, "UP/EX: ", e);
            scheduleUp();
        }
    }

    private static void deleteDirContents(Path dir, boolean alsoDeleteSelf) throws IOException {
        File fDir = dir.toFile();
        if (fDir.exists()) {
            File[] items = fDir.listFiles();
            if (items != null) {
                for (File item : items) {
                    if (item.isDirectory()) {
                        deleteDirContents(item.toPath(), true);
                    }
                    else {
                        if (!item.delete()) {
                            throw new IllegalStateException("Failed to delete: " + item.getAbsolutePath());
                        }
                    }
                }
            }
            if (alsoDeleteSelf && !fDir.delete()) {
                throw new IllegalStateException("Failed to delete: " + fDir.getAbsolutePath());
            }
        }
    }

    @Override
    public String toString() {
        return ChaosUtils.toString(this, System.lineSeparator(), "", "  ", "");
    }

    private ChaosRunner(ChaosArguments a, ChaosPrinter printer) throws IOException {
        if (a.workDirectory == null) {
            a.workDirectory = getTemporaryJetStreamStoreDirBase();
        }
        else if (!a.workDirectory.toFile().exists()) {
            throw new IllegalArgumentException("Work directory does not exist: " + a.workDirectory);
        }

        if (a.servers != 1 && a.servers != 3 && a.servers != 5) {
            throw new IllegalArgumentException("Number of servers must be 1, 3 or 5");
        }

        this.printer = printer;
        this.servers = a.servers;
        this.clusterName = a.clusterName;
        this.serverNamePrefix = a.serverNamePrefix;
        this.js = a.js;
        this.jsStoreDirBase = js ? a.workDirectory : null;
        this.initialDelay = a.initialDelay;
        this.delay = a.delay;
        this.downTime = a.downTime;
        this.random = a.random;
        this.specificPort = a.specificPort;
        this.port = a.port;
        this.listen = a.listen;
        this.monitor = a.monitor;

        natsServerRunners = new ArrayList<>();
        if (servers == 1) {
            if (specificPort != -1 && specificPort != port) {
                throw new IllegalArgumentException("Invalid specific port");
            }
            List<String> inserts = new ArrayList<>();
            ClusterNode cn;
            Path jsStorePath = Paths.get(jsStoreDirBase.toString(), "" + port);
            cn = ClusterNode.builder()
                .port(port)
                .listen(listen)
                .monitor(monitor < 1 ? null : monitor)
                .jsStoreDir(jsStorePath)
                .build();

            if (monitor > 0) {
                inserts.add("http: " + monitor);
            }
            if (js) {
                String storeDir = jsStorePath.toString();
                if (File.separatorChar == '\\') {
                    storeDir = storeDir.replace("\\", "\\\\").replace("/", "\\\\");
                }
                else {
                    storeDir = storeDir.replace("\\", "/");
                }
                inserts.add("jetstream {");
                inserts.add("    store_dir=" + storeDir);
                inserts.add("}");
            }
            inserts.add("server_name=" + serverNamePrefix);

            clusterInserts = new ArrayList<>();
            clusterInserts.add(new ClusterInsert(cn, inserts.toArray(new String[0])));
        }
        else {
            List<ClusterNode> cns = createNodes(servers, clusterName, serverNamePrefix, jsStoreDirBase, DEFAULT_HOST, port, listen, monitor < 1 ? null : monitor);
            if (specificPort != -1) {
                boolean found = false;
                for (ClusterNode cn : cns) {
                    if (cn.port == specificPort) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Invalid specific port");
                }
            }

            clusterInserts = createClusterInserts(cns);
        }

        // delete jsStoreDirs for clean start
        if (js) {
            for (ClusterInsert ci : clusterInserts) {
                deleteDirContents(ci.node.jsStoreDir, false);
            }
        }

        executor = new ScheduledThreadPoolExecutor(2);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);

        // start runners
        for (int i = 0; i < this.servers; i++) {
            try {
                natsServerRunners.add(createRunner(i));
            }
            catch (Exception e) {
                throw new IllegalStateException("Exception Creating Runner", e);
            }
        }

        scheduleDown(initialDelay);
    }

    public static void main(String[] args) {
        start(new ChaosArguments().args(args));
    }

    public static ChaosRunner start(ChaosArguments a) {
        return start(a, null);
    }

    public static ChaosRunner start(ChaosArguments a, ChaosPrinter printer) {
        NatsServerRunner.setDefaultOutputLevel(Level.SEVERE);
        final ChaosPrinter finalPrinter = printer == null ? getDefaultPrinter() : printer;

        try {
            INSTANCE = new ChaosRunner(a, finalPrinter);
        }
        catch (IOException e) {
            finalPrinter.err(CR_LABEL, "Failed to start ChaosRunner", e);
            System.exit(-1);
        }

        Runtime.getRuntime().addShutdownHook(
            new Thread("app-shutdown-hook") {
                @Override
                public void run() {
                    INSTANCE.shutdown();
                    finalPrinter.out(CR_LABEL, "EXIT");
                }
            });

        return INSTANCE;
    }

    private void shutdown() {
        try {
            for (NatsServerRunner runner : natsServerRunners ) {
                try {
                    runner.close();
                }
                catch (Exception ignore) {}
            }
        }
        catch (Exception ignore) {}
    }
}
