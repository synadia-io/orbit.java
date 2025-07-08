// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

import io.nats.ClusterInsert;
import io.nats.ClusterNode;
import io.nats.NatsServerRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.nats.NatsRunnerUtils.*;
import static io.synadia.chaos.ChaosUtils.report;

public class ChaosRunner {

    public final int count;
    public final String clusterName;
    public final String serverNamePrefix;
    public final boolean js;
    public final Path jsStoreDirBase;
    public final long initialDelay;
    public final long delay;
    public final long downTime;
    public final boolean random;

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
        int[] ports = new int[count];
        for (int ix = 0; ix < count; ix++) {
            ports[ix] = clusterInserts.get(ix).node.port;
        }
        return ports;
    }

    public String[] getConnectionUrls() {
        String[] urls = new String[count];
        for (int ix = 0; ix < count; ix++) {
            urls[ix] = getNatsLocalhostUri(clusterInserts.get(ix).node.port);
        }
        return urls;
    }

    public ChaosRunner(int count, String clusterName, String serverNamePrefix, boolean js, Path jsStoreDirBase, long initialDelay, long delay, long downTime, boolean random) throws IOException {
        this.count = count;
        this.clusterName = clusterName;
        this.serverNamePrefix = serverNamePrefix;
        this.js = js;
        this.jsStoreDirBase = jsStoreDirBase;
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.downTime = downTime;
        this.random = random;

        // delete jsStoreDirs for clean start
        List<ClusterNode> nodes = createNodes(count, clusterName, serverNamePrefix, false, jsStoreDirBase);
        clusterInserts = createClusterInserts(nodes);
        for (ClusterInsert ci : clusterInserts) {
            deleteDirContents(ci.node.jsStoreDir, false);
        }

        // start runner
        natsServerRunners = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            try {
                natsServerRunners.add(createRunner(i));
            }
            catch (Exception e) {
                throw new IllegalStateException("Exception Creating Runner", e);
            }
        }

        executor = new ScheduledThreadPoolExecutor(2);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);

        scheduleDown();
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

    private void scheduleDown() {
        executor.schedule(this::downTask, delay, TimeUnit.MILLISECONDS);
    }

    private void scheduleUp() {
        executor.schedule(this::upTask, downTime, TimeUnit.MILLISECONDS);
    }

    private void downTask() {
        try {
            if (random) {
                downIx = ThreadLocalRandom.current().nextInt(count);
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
            NatsServerRunner runner = createRunner(count - 1);
            report("UP", runner.getPort());
            natsServerRunners.add(runner);
            scheduleDown();
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
}
