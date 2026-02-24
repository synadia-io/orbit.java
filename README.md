<p align="center">
  <img src="orbit_shorter.png" alt="Orbit">
</p>

Orbit.java is a set of independent utilities or extensions around the [JNATS](https://github.com/nats-io/nats.java) ecosystem that aims to
boost productivity and provide a higher abstraction layer for the [JNATS](https://github.com/nats-io/nats.java)
client. Note that these libraries will evolve rapidly and API guarantees are general not made until the specific project has a v1.0.0 version.

# Utilities
| Module                       | Description                                             | Docs                                         | Release Version | Snapshot       |
|------------------------------|---------------------------------------------------------|----------------------------------------------|-----------------|----------------|
| Retrier                      | Extension for retrying anything                         | [README.md](retrier/README.md)               | 0.2.1           | 0.2.2-SNAPSHOT |
| Jetstream Publish Extensions | General extensions for Jetstream Publishing             | [README.md](js-publish-extensions/README.md) | 0.4.4           | 0.4.5-SNAPSHOT |
| Request Many                 | Get many responses for a single core request.           | [README.md](request-many/README.md)          | 0.1.1           | 0.1.2-SNAPSHOT |
| Encoded KeyValue             | Allow custom encoding of keys and values.               | [README.md](encoded-kv/README.md)            | 0.1.1           | 0.1.2-SNAPSHOT |
| Direct Batch                 | Leverages direct message capabilities in NATS Server    | [README.md](direct-batch/README.md)          | 0.0.4           | 0.0.5-SNAPSHOT |
| Batch Publish                | Publish an atomic batch                                 | [README.md](batch-publish/README.md)         | 0.0.0           | 0.0.0-SNAPSHOT |
| Distributed Counters         | Leverage distributed counters functionality             | [README.md](counters/README.md)              | 0.2.2           | 0.2.3-SNAPSHOT |
| Scheduled Message            | Leverage ability to schedule a message                  | [README.md](schedule-message/README.md)      | 0.0.3           | 0.0.4-SNAPSHOT |
| Chaos Runner                 | Run some NATS servers and cause chaos                   | [README.md](chaos-runner/README.md)          | 0.0.8           | 0.0.9-SNAPSHOT |
| Partitioned Consumer Groups  | Partitioned Consumer Group funcitionality for JetStream | [README.md](pcgroups/README.md)              | 0.0.1           | 0.0.1-SNAPSHOT |

## Retrier

Extension for retrying anything.

[Retrier README](retrier/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:retrier-197556?labelColor=grey&style=flat)
![0.2.1](https://img.shields.io/badge/Current_Release-0.2.1-27AAE0)
![0.2.2](https://img.shields.io/badge/Current_Snapshot-0.2.2--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/retrier/javadoc.svg)](https://javadoc.io/doc/io.synadia/retrier)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/retrier)](https://img.shields.io/maven-central/v/io.synadia/retrier)

## Jetstream Publish Extensions

General extensions for Jetstream Publishing

[Jetstream Publish Extensions README](js-publish-extensions/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:jnats--js--publish--extensions-197556?labelColor=grey&style=flat)
![0.4.4](https://img.shields.io/badge/Current_Release-0.4.4-27AAE0)
![0.4.5](https://img.shields.io/badge/Current_Snapshot-0.4.5--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/jnats-js-publish-extensions/javadoc.svg)](https://javadoc.io/doc/io.synadia/jnats-js-publish-extensions)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/jnats-js-publish-extensions)](https://img.shields.io/maven-central/v/io.synadia/jnats-js-publish-extensions)

## Request Many

Extension to get many responses for a single core request.

[Request Many README](request-many/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:request--many-197556?labelColor=grey&style=flat)
![0.1.1](https://img.shields.io/badge/Current_Release-0.1.1-27AAE0)
![0.1.2](https://img.shields.io/badge/Current_Snapshot-0.1.2--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/request-many/javadoc.svg)](https://javadoc.io/doc/io.synadia/request-many)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/request-many)](https://img.shields.io/maven-central/v/io.synadia/request-many)

## Encoded KeyValue

Extension over Key Value to allow custom encoding of keys and values.

[Encoded KeyValue README](encoded-kv/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:encoded--kv-197556?labelColor=grey&style=flat)
![0.0.4](https://img.shields.io/badge/Current_Release-0.0.4-27AAE0)
![0.0.5](https://img.shields.io/badge/Current_Snapshot-0.0.5--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/encoded-kv/javadoc.svg)](https://javadoc.io/doc/io.synadia/encoded-kv)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/encoded-kv)](https://img.shields.io/maven-central/v/io.synadia/encoded-kv)

## Direct Batch

The direct batch functionality leverages the direct message capabilities introduced in NATS Server v2.11.
The functionality is described in [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md)

[Direct Batch README](direct-batch/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--batch-197556?labelColor=grey&style=flat)
![0.1.4](https://img.shields.io/badge/Current_Release-0.1.4-27AAE0)
![0.1.5](https://img.shields.io/badge/Current_Snapshot-0.1.5--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-batch/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-batch)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/direct-batch)](https://img.shields.io/maven-central/v/io.synadia/direct-batch)

### Batch Publish

Utility to publish an atomic batch, a group of up to 1000 messages

[Batch Publish README](batch-publish/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:batch--publish-197556?labelColor=grey&style=flat)
![0.2.2](https://img.shields.io/badge/Current_Release-0.2.2-27AAE0)
![0.2.3](https://img.shields.io/badge/Current_Snapshot-0.2.3--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/batch-publish/javadoc.svg)](https://javadoc.io/doc/io.synadia/batch-publish)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/batch-publish)](https://img.shields.io/maven-central/v/io.synadia/batch-publish)

### Distributed Counters

Utility to take leverage the distributed counter functionality.

[Distributed Counters README](counters/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:counters-197556?labelColor=grey&style=flat)
![0.2.2](https://img.shields.io/badge/Current_Release-0.2.2-27AAE0)
![0.2.3](https://img.shields.io/badge/Current_Snapshot-0.2.3--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/counters/javadoc.svg)](https://javadoc.io/doc/io.synadia/counters)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/counters)](https://img.shields.io/maven-central/v/io.synadia/counters)

### Schedule Message

Utility to leverage the ability to schedule a message to be published at a later time.
Eventually the ability to schedule a message to publish based on a cron or schedule.

[Scheduled Message README](schedule-message/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:schedule--message-197556?labelColor=grey&style=flat)
![0.0.3](https://img.shields.io/badge/Current_Release-0.0.3-27AAE0)
![0.0.4](https://img.shields.io/badge/Current_Snapshot-0.0.4--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/schedule-message/javadoc.svg)](https://javadoc.io/doc/io.synadia/schedule-message)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/schedule-message)](https://img.shields.io/maven-central/v/io.synadia/schedule-message)

## Chaos Runner

Run some NATS servers and cause chaos by bringing them up and down.

[Chaos Runner README](chaos-runner/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:chaos--runner-197556?labelColor=grey&style=flat)
![0.0.8](https://img.shields.io/badge/Current_Release-0.0.8-27AAE0)
![0.0.9](https://img.shields.io/badge/Current_Snapshot-0.0.9--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/chaos-runner/javadoc.svg)](https://javadoc.io/doc/io.synadia/chaos-runner)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/chaos-runner)](https://img.shields.io/maven-central/v/io.synadia/chaos-runner)

## Partitioned Consumer Groups

Implementation of the partitioned Consumer Group functionality, ported from and compatible with the [Golang version](https://github.com/synadia-io/orbit.go/tree/main/pcgroups).

[Partitioned Consumer Groups README](pcgroups/README.md)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:pcgroups-197556?labelColor=grey&style=flat)
![0.1.0](https://img.shields.io/badge/Current_Release-0.1.0-27AAE0)
![0.1.1](https://img.shields.io/badge/Current_Snapshot-0.1.1--SNAPSHOT-27AAE0)
[![javadoc](https://javadoc.io/badge2/io.synadia/pcgroups/javadoc.svg)](https://javadoc.io/doc/io.synadia/pcgroups)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/pcgroups)](https://img.shields.io/maven-central/v/io.synadia/pcgroups)

# Dependencies
### Gradle

The libraries are available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

```groovy
dependencies {
    implementation 'io.synadia:{artifact}:{major.minor.patch}'
}
```

Releases are available at Maven Central:

```groovy
repositories {
    mavenCentral()
}
```

If you need a snapshot version, you must add the url for the snapshots.

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://central.sonatype.com/repository/maven-snapshots/"
    }
}

dependencies {
   implementation 'io.synadia:{artifact}:{major.minor.patch}-SNAPSHOT'
}
```
### Maven

The libraries are available on the Maven central repository, and can be imported as a normal dependency in your `pom.xml` file:

```xml
<dependency>
    <groupId>io.synadia</groupId>
    <artifactId>{artifact}</artifactId>
    <version>{major.minor.patch}</version>
</dependency>
```

Releases are available at Maven Central.
If you need a snapshot version, you must enable snapshots and change your dependency.

```xml
<repositories>
    <repository>
        <name>Central Portal Snapshots</name>
        <id>central-portal-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependency>
    <groupId>io.synadia</groupId>
    <artifactId>{artifact}</artifactId>
    <version>{major.minor.patch}-SNAPSHOT</version>
</dependency>
```

# Notes

If you are importing the source code from this repo, please be aware that each project is its own library. Some projects have classes with the same name,
but each project is completely independent on another,
except if one specifically depends on another. 
For example, the publish extensions depends on retrier, but it includes the library via gradle, not the source code.
