package io.synadia.chaos;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.nats.NatsRunnerUtils.DEFAULT_CLUSTER_NAME;
import static io.nats.NatsRunnerUtils.DEFAULT_SERVER_NAME_PREFIX;

public class ChaosArguments {

    int servers = 3;
    String clusterName = DEFAULT_CLUSTER_NAME;
    String serverNamePrefix = DEFAULT_SERVER_NAME_PREFIX;
    boolean js = true;
    Path workDirectory;
    long initialDelay = 30_000;
    long delay = 5_000;
    long downTime = 5_000;
    boolean random = false;
    int port = 4222;
    int listen = 4232;
    int monitor = 4282;

    public ChaosArguments servers(int servers) {
        this.servers = servers;
        return this;
    }

    public ChaosArguments clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public ChaosArguments serverNamePrefix(String serverNamePrefix) {
        this.serverNamePrefix = serverNamePrefix;
        return this;
    }

    public ChaosArguments js(boolean js) {
        this.js = js;
        return this;
    }

    public ChaosArguments workDirectory(String workDirectory) {
        if (workDirectory == null || workDirectory.trim().isEmpty()) {
            this.workDirectory = null;
            return this;
        }
        return workDirectory(Paths.get(workDirectory));
    }

    public ChaosArguments workDirectory(File workDirectory) {
        if (workDirectory == null) {
            this.workDirectory = null;
            return this;
        }
        return workDirectory(workDirectory.getPath());
    }

    public ChaosArguments workDirectory(Path workDirectory) {
        if (workDirectory == null) {
            this.workDirectory = null;
            return this;
        }
        this.workDirectory = workDirectory;
        return this;
    }

    public ChaosArguments initialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
        return this;
    }

    public ChaosArguments delay(long delay) {
        this.delay = delay;
        return this;
    }

    public ChaosArguments downTime(long downTime) {
        this.downTime = downTime;
        return this;
    }

    public ChaosArguments random(boolean random) {
        this.random = random;
        return this;
    }

    public ChaosArguments port(int port) {
        this.port = port;
        return this;
    }

    public ChaosArguments listen(int listen) {
        this.listen = listen;
        return this;
    }

    public ChaosArguments monitor(int monitor) {
        this.monitor = monitor;
        return this;
    }

    public ChaosArguments args(String[] args) {
        if (args != null && args.length > 0) {
            try {
                for (int x = 0; x < args.length; x++) {
                    String arg = args[x].trim();
                    switch (arg) {
                        case "--servers":
                            servers(Integer.parseInt(args[++x]));
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
                        case "--nojs":
                            js(false);
                            break;
                        case "--random":
                            random(true);
                            break;
                        case "--port":
                            port(Integer.parseInt(args[++x]));
                            break;
                        case "--listen":
                            listen(Integer.parseInt(args[++x]));
                            break;
                        case "--monitor":
                            monitor(Integer.parseInt(args[++x]));
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

    public int getServers() {
        return servers;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getServerNamePrefix() {
        return serverNamePrefix;
    }

    public boolean isJs() {
        return js;
    }

    public Path getWorkDirectory() {
        return workDirectory;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public long getDelay() {
        return delay;
    }

    public long getDownTime() {
        return downTime;
    }

    public boolean isRandom() {
        return random;
    }

    public int getPort() {
        return port;
    }

    public int getListen() {
        return listen;
    }

    public int getMonitor() {
        return monitor;
    }
}
