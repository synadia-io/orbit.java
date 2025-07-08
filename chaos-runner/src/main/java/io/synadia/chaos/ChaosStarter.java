package io.synadia.chaos;

import io.nats.NatsServerRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;

import static io.nats.NatsRunnerUtils.*;
import static io.synadia.chaos.ChaosUtils.report;

public class ChaosStarter {

    int count = 3;
    String clusterName = DEFAULT_CLUSTER_NAME;
    String serverNamePrefix = DEFAULT_SERVER_NAME_PREFIX;
    boolean js = true;
    Path workDirectory;
    long initialDelay = 10_000;
    long delay = 5_000;
    long downTime = 5_000;
    boolean random = false;

    private static ChaosRunner INSTANCE;

    public ChaosStarter count(int count) {
        this.count = count;
        return this;
    }

    public ChaosStarter clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public ChaosStarter serverNamePrefix(String serverNamePrefix) {
        this.serverNamePrefix = serverNamePrefix;
        return this;
    }

    public ChaosStarter js(boolean js) {
        this.js = js;
        return this;
    }

    public ChaosStarter workDirectory(String workDirectory) {
        return workDirectory(Paths.get(workDirectory));
    }

    public ChaosStarter workDirectory(File workDirectory) {
        return workDirectory(workDirectory.getPath());
    }

    public ChaosStarter workDirectory(Path workDirectory) {
        this.workDirectory = workDirectory;
        return this;
    }

    public ChaosStarter initialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
        return this;
    }

    public ChaosStarter delay(long delay) {
        this.delay = delay;
        return this;
    }

    public ChaosStarter downTime(long downTime) {
        this.downTime = downTime;
        return this;
    }

    public ChaosStarter random(boolean random) {
        this.random = random;
        return this;
    }

    public ChaosStarter args(String[] args) {
        if (args != null && args.length > 0) {
            try {
                for (int x = 0; x < args.length; x++) {
                    String arg = args[x].trim();
                    switch (arg) {
                        case "--count":
                            count(Integer.parseInt(args[++x]));
                            break;
                        case "--delay":
                            delay(Long.parseLong(args[++x]));
                            break;
                        case "--initial":
                            initialDelay(Long.parseLong(args[++x]));
                            break;
                        case "--down":
                            downTime(Long.parseLong(args[++x]));
                            break;
                        case "--cname":
                            clusterName(args[++x]);
                            break;
                        case "--prefix":
                            serverNamePrefix(args[++x]);
                            break;
                        case "--dir":
                            workDirectory(args[++x]);
                            break;
                        case "--js":
                            js(true);
                            break;
                        case "--nojs":
                            js(false);
                            break;
                        case "--random":
                            random(true);
                            break;
                        case "--norandom":
                            random(false);
                            break;
                        case "":
                            break;
                        default:
                            error("Unknown argument: " + arg);
                            break;
                    }
                }
            }
            catch (Exception e) {
                error("Exception while parsing, most likely missing an argument value.");
            }
        }
        return this;
    }

    public void error(String errMsg) {
        System.err.println("ERROR: " + errMsg);
        System.exit(-1);
    }

    public ChaosRunner start() throws IOException {
        NatsServerRunner.setDefaultOutputLevel(Level.SEVERE);

        if (INSTANCE == null) {
            if (workDirectory == null) {
                workDirectory = getTemporaryJetStreamStoreDirBase();
            }
            else if (!workDirectory.toFile().exists()) {
                throw new IllegalArgumentException("Work directory does not exist: " + workDirectory);
            }

            Path jsStoreDirBase = js ? workDirectory : null;

            Runtime.getRuntime().addShutdownHook(
                new Thread("app-shutdown-hook") {
                    @Override
                    public void run() {
                        INSTANCE.shutdown();
                        report("EXIT Chaos Runner");
                    }
                });

            INSTANCE = new ChaosRunner(
                count,
                clusterName,
                serverNamePrefix,
                js,
                jsStoreDirBase,
                initialDelay,
                delay,
                downTime,
                random);

            report("STARTER", INSTANCE);
        }
        else {
            report("STARTER", "Chaos Runner previously started.", INSTANCE);
        }
        return INSTANCE;
    }
}
