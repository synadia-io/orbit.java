// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

import io.nats.ClusterDefaults;
import io.nats.ClusterInsert;
import io.nats.ClusterNode;
import io.nats.JsConfig;
import io.nats.NatsRunnerUtils;
import io.nats.NatsServerRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import static io.nats.ClusterUtils.createClusterInserts;
import static io.nats.ClusterUtils.createNodes;
import static io.nats.NatsRunnerUtils.getNatsLocalhostUri;
import static io.synadia.chaos.ChaosUtils.getDefaultPrinter;

public class ChaosRunner {

    private static final String CR_LABEL = "ChaosRunner";

    private static final ReentrantLock INSTANCE_LOCK = new ReentrantLock();
    private static ChaosRunner INSTANCE;
    private static ChaosArguments INSTANCE_ARGUMENTS;
    private static Thread APP_SHUTDOWN_HOOK_THREAD;

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
            .jetstream(js)
            .configInserts(ci.configInserts)
            .port(ci.node.port)
            .skipConnectValidate();
        return b.build();
    }

    private void scheduleDown(long delay) {
        executor.schedule(this::downTask, delay, TimeUnit.MILLISECONDS);
    }

    private void scheduleUp() {
        executor.schedule(this::upTask, downTime, TimeUnit.MILLISECONDS);
    }

    private void downTask() {
        // natsServerRunners is an ArrayList and shutdownServers() iterates it under this
        // lock. executor.shutdown() does not interrupt a task already running, so without
        // the lock a structural change here can race that iteration.
        INSTANCE_LOCK.lock();
        try {
            if (INSTANCE == null) {
                // shut down before this task got to run
                return;
            }
            if (specificPort != -1) {
                for (int i = 0; i < natsServerRunners.size(); i++) {
                    NatsServerRunner nsr = natsServerRunners.get(i);
                    if (nsr.getNatsPort() == specificPort) {
                        downIx = i;
                        break;
                    }
                }
            }
            else if (random) {
                downIx = ThreadLocalRandom.current().nextInt(servers);
            }

            NatsServerRunner runner = natsServerRunners.remove(downIx);
            printer.out(CR_LABEL, "DOWN", runner.getNatsPort());
            clusterInserts.add(clusterInserts.remove(downIx));
            runner.close();
            scheduleUp();
        }
        catch (Throwable e) {
            printer.out(CR_LABEL, "DOWN/EX", e);
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }

    private void upTask() {
        try {
            NatsServerRunner runner = createRunner(servers - 1);
            INSTANCE_LOCK.lock();
            try {
                if (INSTANCE == null) {
                    // Shut down while this server was starting. executor.shutdown() does not
                    // interrupt a task already running, so we got here after shutdownServers()
                    // had already closed out the list and the jvm hook was removed. Close it
                    // here, or it outlives the jvm still holding its port.
                    try { runner.close(); } catch (Exception ignore) {}
                    return;
                }
                printer.out(CR_LABEL, "UP", runner.getNatsPort());
                natsServerRunners.add(runner);
                scheduleDown(delay);
            }
            finally {
                INSTANCE_LOCK.unlock();
            }
        }
        catch (Throwable e) {
                printer.out(CR_LABEL, "UP/EX: ", e);
            scheduleUp();
        }
    }

    private static void deleteDirContents(Path dir, boolean alsoDeleteSelf) {
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
            a.workDirectory = Files.createTempDirectory(null);
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
            // jsStoreDirBase is only set when js is on, and ClusterNode takes a null
            // jsStoreDir, which is what the cluster branch ends up with in that case too
            Path jsStorePath = js ? Paths.get(jsStoreDirBase.toString(), "" + port) : null;
            ClusterNode cn = ClusterNode.builder()
                .port(port)
                .listen(listen)
                .monitor(monitor < 1 ? null : monitor)
                .jsStoreDir(jsStorePath)
                .build();

            if (monitor > 0) {
                inserts.add("http: " + monitor);
            }
            if (js) {
                // as of jnats-server-runner 4.0.2 JsConfig cleans and escapes the dir it is
                // given, the same as the cluster branch gets by way of createClusterInserts
                inserts.addAll(new JsConfig(jsStorePath).configInserts);
            }
            inserts.add("server_name=" + serverNamePrefix);

            clusterInserts = new ArrayList<>();
            clusterInserts.add(new ClusterInsert(cn, inserts.toArray(new String[0])));
        }
        else {
            ClusterDefaults cd = new ClusterDefaults()
                .count(servers)
                .clusterName(clusterName)
                .serverNamePrefix(serverNamePrefix)
                .host(NatsRunnerUtils.LocalHost.ip.host)
                .portStart(port)
                .listenStart(listen)
                .monitorStart(monitor); // less than 1 turns the monitor off
            List<ClusterNode> cns = createNodes(cd, jsStoreDirBase);
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
                // jsStoreDir is nullable on ClusterNode, so it might not have been given one
                if (ci.node.jsStoreDir != null) {
                    deleteDirContents(ci.node.jsStoreDir, false);
                }
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

    public static ChaosRunner start(ChaosArguments a) throws Exception {
        return start(a, null);
    }

    public static ChaosRunner start(ChaosArguments a, ChaosPrinter printer) throws Exception {
        NatsRunnerUtils.setDefaultOutputLevel(Level.SEVERE);
        final ChaosPrinter finalPrinter = printer == null ? getDefaultPrinter() : printer;

        INSTANCE_LOCK.lock();
        try {
            if (INSTANCE != null) {
                if (INSTANCE_ARGUMENTS.equals(a)) {
                    // same arguments, just return the instance
                    return INSTANCE;
                }

                throw new Exception("Instance already started with different arguments.");
            }

            INSTANCE = new ChaosRunner(a, finalPrinter);
            INSTANCE_ARGUMENTS = a;

            APP_SHUTDOWN_HOOK_THREAD = new Thread("app-shutdown-hook") {
                @Override
                public void run() {
                    shutdownExecutor();
                    shutdownServers();
                    finalPrinter.out(CR_LABEL, "EXIT");
                }
            };

            Runtime.getRuntime().addShutdownHook(APP_SHUTDOWN_HOOK_THREAD);

        }
        catch (IOException e) {
            finalPrinter.err(CR_LABEL, "Failed to start ChaosRunner", e);
            throw e;
        }
        finally {
            INSTANCE_LOCK.unlock();
        }

        return INSTANCE;
    }

    public static boolean isRunning() {
        INSTANCE_LOCK.lock();
        try {
            return INSTANCE != null;
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }

    public static void shutdown() {
        INSTANCE_LOCK.lock();
        try {
            removeShutdownHook();
            shutdownExecutor();
            shutdownServers();
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }

    public static void shutdownExecutor() {
        INSTANCE_LOCK.lock();
        try {
            // guard matches shutdownServers(). This is public and is also reachable a
            // second time when the jvm hook and an explicit shutdown() overlap.
            if (INSTANCE != null) {
                INSTANCE.executor.shutdown();
            }
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }

    private static void removeShutdownHook() {
        INSTANCE_LOCK.lock();
        try {
            if (APP_SHUTDOWN_HOOK_THREAD != null) {
                Runtime.getRuntime().removeShutdownHook(APP_SHUTDOWN_HOOK_THREAD);
                APP_SHUTDOWN_HOOK_THREAD = null;
            }
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }

    private static void shutdownServers() {
        INSTANCE_LOCK.lock();
        try {
            if (INSTANCE != null) {
                for (NatsServerRunner runner : INSTANCE.natsServerRunners) {
                    try { runner.close(); } catch (Exception ignore) {}
                }
                INSTANCE = null;
                INSTANCE_ARGUMENTS = null;
            }
        }
        finally {
            INSTANCE_LOCK.unlock();
        }
    }
}
