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
import static io.synadia.chaos.ChaosUtils.report;

public class ChaosRunner {

    public final int servers;
    public final String clusterName;
    public final String serverNamePrefix;
    public final boolean js;
    public final Path jsStoreDirBase;
    public final long initialDelay;
    public final long delay;
    public final long downTime;
    public final boolean random;
    public final int port;
    public final int listen;

    private final List<ClusterInsert> clusterInserts;
    private final List<NatsServerRunner> natsServerRunners;
    private final ScheduledThreadPoolExecutor executor;
    private int downIx = 0;

    public void shutdown() {
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

    public int[] getConnectionPorts() {
        int[] ports = new int[servers];
        for (int ix = 0; ix < servers; ix++) {
            ports[ix] = clusterInserts.get(ix).node.port;
        }
        return ports;
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
            if (random) {
                downIx = ThreadLocalRandom.current().nextInt(servers);
            }

            NatsServerRunner runner = natsServerRunners.remove(downIx);
            report("DOWN", runner.getPort());
            clusterInserts.add(clusterInserts.remove(downIx));
            runner.close();

            scheduleUp();
        }
        catch (Throwable e) {
            report("DOWN/EX", e);
        }
    }

    private void upTask() {
        try {
            NatsServerRunner runner = createRunner(servers - 1);
            report("UP", runner.getPort());
            natsServerRunners.add(runner);
            scheduleDown(delay);
        }
        catch (Throwable e) {
            report("UP/EX: ", e);
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

    private static ChaosRunner INSTANCE;

    private ChaosRunner(ChaosArguments a) throws IOException {
        if (a.workDirectory == null) {
            a.workDirectory = getTemporaryJetStreamStoreDirBase();
        }
        else if (!a.workDirectory.toFile().exists()) {
            throw new IllegalArgumentException("Work directory does not exist: " + a.workDirectory);
        }

        if (a.servers != 1 && a.servers != 3 && a.servers != 5) {
            throw new IllegalArgumentException("Number of servers must be 1, 3 or 5");
        }

        this.servers = a.servers;
        this.clusterName = a.clusterName;
        this.serverNamePrefix = a.serverNamePrefix;
        this.js = a.js;
        this.jsStoreDirBase = js ? a.workDirectory : null;
        this.initialDelay = a.initialDelay;
        this.delay = a.delay;
        this.downTime = a.downTime;
        this.random = a.random;
        this.port = a.port;
        this.listen = a.listen;

        natsServerRunners = new ArrayList<>();
        if (servers == 1) {
            List<String> inserts = new ArrayList<>();
            ClusterNode cn;
            if (jsStoreDirBase == null) {
                cn = null;
            }
            else {
                Path jsStorePath = Paths.get(jsStoreDirBase.toString(), "" + port);
                cn = ClusterNode.builder()
                    .port(port)
                    .jsStoreDir(jsStorePath)
                    .build();

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
            List<ClusterNode> cns = createNodes(servers, clusterName, serverNamePrefix, jsStoreDirBase, DEFAULT_HOST, port, listen, null);
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
        NatsServerRunner.setDefaultOutputLevel(Level.SEVERE);
        try {
            INSTANCE = new ChaosRunner(a);
        }
        catch (IOException e) {
            System.out.println("Failed to start ChaosRunner: " + e.getMessage());
            System.exit(-1);
        }
        System.out.println(ChaosUtils.toString(INSTANCE, System.lineSeparator(), "[" + System.currentTimeMillis() + "] ", "  ", ""));

        Runtime.getRuntime().addShutdownHook(
            new Thread("app-shutdown-hook") {
                @Override
                public void run() {
                    INSTANCE.shutdown();
                    report("EXIT Chaos Runner");
                }
            });

        return INSTANCE;
    }
}
