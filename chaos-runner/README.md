![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Chaos Runner

A simple java program that can start 1 or more NATS Servers and then add chaos,
by taking one of them down on a delay and bringing it back up after a downtime.

**Current Release**: N/A
&nbsp; **Current Snapshot**: 0.0.2-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:chaos-runner`

[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

## Usage

The easier way to use is to just get the chaos-runner source directory from this repo and build it yourself via the 
```
gradle uberJar
```
task from inside the chaos-runner folder, and then run the program from a command line with arguments.
Alternatively you can run a program like the [ChaosRunnerExample](src/examples/java/io/synadia/examples/ChaosRunnerExample.java)

## Command Line Arguments

| Argument             | Description                                                                | Default     |
|----------------------|----------------------------------------------------------------------------|-------------|
| `--servers <number>` | Number of servers. Accepts 1, 3 or 5                                       | 3           |
| `--delay <millis>`   | Delay to bring down a server, since all servers were up.                   | 5000        |
| `--initial <millis>` | The first delay. Gives time to start your test program and run setup.      | 30000       |
| `--down <millis>`    | Delay to bring a server up once it is brought down.                        | 5000        |
| `--cname <name>`     | Cluster name. Ignored for 1 server.                                        | "cluster"   |
| `--prefix <name>`    | Prefix to use for the server name. Used in it's entirety for 1 server      | "server"    |
| `--dir <path>`       | The working dir. Used as the parent dir for JetStream storage directories. | _temp_      |
| `--nojs`             | Do not run the server with JetStream. JetStream is on by default.          | JetStream   |
| `--random`           | Take the servers down randomly. Default is Round Robin.                    | Round Robin |
| `--port`             | The starting port.                                                         | 4220        |
| `--listen`           | The starting listen port for clusters.                                     | 4230        |

Regarding ports. Given any starting port, the system automatically figures the ports for the other nodes.
For example for 3 nodes, if the starting port is 4220, other ports are 4221 and 4222. 
If the listen port is 4230, the other listen ports are 4231 and 4232 

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:chaos--runner-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner)
[![javadoc](https://javadoc.io/badge2/io.synadia/chaos-runner/javadoc.svg)](https://javadoc.io/doc/io.synadia/chaos-runner)


## Command Line Examples

Assuming you have just run gradle from the command line inside the chaos-runner directory...

```
java -cp build/libs/chaos-runner-0.0.2-uber.jar io.synadia.chaos.ChaosRunner --delay 4000 --initial 10000 --cname mycluster --prefix myserver
```

```
java -cp build/libs/chaos-runner-0.0.2-uber.jar io.synadia.chaos.ChaosRunner --servers 1 --delay 4000 --initial 10000
```

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
