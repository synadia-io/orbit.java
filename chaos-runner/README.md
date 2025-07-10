![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Chaos Runner

A simple java program that can start 1 or more NATS Servers and then add chaos,
by taking one of them down on a delay and bringing it back up after a downtime.

**Current Release**: 0.0.2
&nbsp; **Current Snapshot**: 0.0.3-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:chaos-runner`

[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

## Uber Jar

The project builds an Uber Jar that contains the compiled code for the Chaos Runner and the Nats Server Runner.
You can get this jar in 2 ways.

1. Download the release: [chaos-runner-0.0.2-uber.jar](https://repo1.maven.org/maven2/io/synadia/chaos-runner/0.0.2/chaos-runner-0.0.2-uber.jar)

2. Build from the source. Get the entire chaos-runner source from this Orbit repo, 
   and from the chaos-runner project directory and run `gradle uberJar`
   The Uber Jar `chaos-runner-0.0.2-SNAPSHOT-uber.jar` will appear in the `build/libs/` directory
   (relative to the `chaos-runner` project directory.)

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
| `--port`             | The starting server port.                                                  | 4220        |
| `--listen`           | The starting listen port for clusters.                                     | 4230        |

#### Regarding ports 
Given any starting port, the system automatically figures the ports for the other nodes.
For example for 3 nodes:
* if the starting server port is 4220, the other ports are 4221 and 4222. 
* if the listen port is 4230, the other listen ports are 4231 and 4232 

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:chaos--runner-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner)
[![javadoc](https://javadoc.io/badge2/io.synadia/chaos-runner/javadoc.svg)](https://javadoc.io/doc/io.synadia/chaos-runner)


## Command Line Examples

```
java -cp <Path-To>/<Jar-Name> io.synadia.chaos.ChaosRunner --delay 4000 --initial 10000 --cname mycluster --prefix myserver
java -cp <Path-To>/<Jar-Name> io.synadia.chaos.ChaosRunner --servers 1 --delay 4000 --initial 10000
```

#### Path-To and Jar-Name
1\.If you downloaded the Uber Jar release: 
* the `<Path-To>` will be wherever you stored the file.
* The `<Jar-Name>` will be `chaos-runner-0.0.2-uber.jar`.

2\. If you build it yourself:
* the `<Path-To>` will be relative to the `chaos-runner` directory in `build/libs`
* the `<Jar-Name>` will be `chaos-runner-0.0.2-SNAPSHOT-uber.jar`.

## Other ways to run... 

Alternatively you can run a program like the [ChaosRunnerExample](src/examples/java/io/synadia/examples/ChaosRunnerExample.java) from an ide.

### Running native image

You can use [GraalVM](https://www.graalvm.org/) native-image to create native executable for your platform:
```
# install GraalVM
> native-image.cmd -cp <Path-To>\chaos-runner-0.0.2-uber.jar io.synadia.chaos.ChaosRunner chaos-runner
> .\chaos-runner.exe --servers 1 --delay 4000 --initial 10000
```

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
