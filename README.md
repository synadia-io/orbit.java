<p align="center">
  <img src="orbit_shorter.png" alt="Orbit">
</p>

Orbit.java is a set of independent utilities or extensions around the [JNATS](https://github.com/nats-io/nats.java) ecosystem that aims to
boost productivity and provide a higher abstraction layer for the [JNATS](https://github.com/nats-io/nats.java)
client. Note that these libraries will evolve rapidly and API guarantees are general not made until the specific project has a v1.0.0 version.

# Utilities

## Retrier

Extension for retrying anything. 

**Current Release**: 0.2.1
&nbsp; **Current Snapshot**: 0.2.2-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=retrier/README.md)](retrier/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:retrier-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/retrier/javadoc.svg)](https://javadoc.io/doc/io.synadia/retrier)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier)                                         

## Jetstream Publish Extensions

General extensions for Jetstream Publishing

**Current Release**: 0.4.3
&nbsp; **Current Snapshot**: 0.4.3-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=js-publish-extensions/README.md)](js-publish-extensions/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:jnats--js--publish--extensions-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/jnats-js-publish-extensions/javadoc.svg)](https://javadoc.io/doc/io.synadia/jnats-js-publish-extensions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions)

## Request Many

Extension to get many responses for a single core request.

**Current Release**: 0.1.0
&nbsp; **Current Snapshot**: 0.1.1-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=request-many/README.md)](request-many/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:request--many-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/request-many/javadoc.svg)](https://javadoc.io/doc/io.synadia/request-many)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/request-many/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/request-many)

## Encoded KeyValue

Extension over Key Value to allow custom encoding of keys and values.

**Current Release**: 0.0.1
&nbsp; **Current Snapshot**: 0.0.2-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=encoded-kv/README.md)](encoded-kv/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:encoded--kv-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/encoded-kv/javadoc.svg)](https://javadoc.io/doc/io.synadia/encoded-kv)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/encoded-kv/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/encoded-kv)

## Direct Batch

The direct batch functionality leverages the direct message capabilities introduced in NATS Server v2.11.
The functionality is described in [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md)

**Current Release**: 0.1.3
&nbsp;**Current Snapshot**: 0.1.4-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=direct-batch/README.md)](direct-batch/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--batch-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-batch/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-batch)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-batch/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-batch)

## Chaos Runner

Run some NATS servers and cause chaos by bringing them up and down.

**Current Release**: 0.0.3
&nbsp; **Current Snapshot**: 0.0.4-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=chaos-runner/README.md)](chaos-runner/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:chaos--runner-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/chaos-runner/javadoc.svg)](https://javadoc.io/doc/io.synadia/chaos-runner)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/chaos-runner)

### Batch Publish

Utility to publish an atomic batch, a group of up to 1000 messages

**Current Release**: 0.2.0
&nbsp;**Current Snapshot**: 0.2.1-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=batch-publish/README.md)](batch-publish/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:batch--publish-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/batch-publish/javadoc.svg)](https://javadoc.io/doc/io.synadia/batch-publish)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/batch-publish/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/batch-publish)

### JetStream Distributed Counters CRDT

Utility to take advantage of the distributed counter functionality.

**Current Release**: 0.1.0
&nbsp; **Current Snapshot**: 0.1.1-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=counter/README.md)](counter/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:counter-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/counter/javadoc.svg)](https://javadoc.io/doc/io.synadia/counter)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/counter/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/counter)

### JetStream Scheduled Message

Utility to leverage the ability to schedule a message to be published at a later time.
Eventually the ability to schedule a message to publish based on a cron or schedule.

**Current Release**: 0.0.1
&nbsp; **Current Snapshot**: 0.0.2-SNAPSHOT

[![README](https://img.shields.io/badge/README-blue?style=flat&link=schedule-message/README.md)](schedule-message/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:scheduled--message-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/scheduled-message/javadoc.svg)](https://javadoc.io/doc/io.synadia/scheduled-message)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/scheduled-message/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/scheduled-message)

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
