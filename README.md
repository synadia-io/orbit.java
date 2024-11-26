<p align="center">
  <img src="orbit.png" alt="Orbit">
</p>

Orbit.java is a set of independent utilities or extensions around the [JNATS](https://github.com/nats-io/nats.java) ecosystem that aims to
boost productivity and provide higher abstraction layer for the [JNATS](https://github.com/nats-io/nats.java)
client. Note that these libraries will evolve rapidly and API guarantees are
not made until the specific project has a v1.0.0 version.

# Utilities

## Retrier

Extension for retrying anything. 

[![README](https://img.shields.io/badge/README-blue?style=flat&link=retrier/README.md)](retrier/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:retrier-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/retrier/javadoc.svg)](https://javadoc.io/doc/io.synadia/retrier)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/retrier)                                         

## JS Publish Extensions

Extensions around Jetstream Publishing

[![README](https://img.shields.io/badge/README-blue?style=flat&link=js-publish-extensions/README.md)](js-publish-extensions/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:jnats--js--publish--extensions-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/jnats-js-publish-extensions/javadoc.svg)](https://javadoc.io/doc/io.synadia/jnats-js-publish-extensions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions)

## Request Many

Extension to get many response for a single core request.

[![README](https://img.shields.io/badge/README-blue?style=flat&link=request-many/README.md)](request-many/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:request--many-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/request-many/javadoc.svg)](https://javadoc.io/doc/io.synadia/request-many)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/request-many/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/request-many)

## Direct Consumer

Extension to get many response for a single core request.

[![README](https://img.shields.io/badge/README-blue?style=flat&link=direct-consumer/README.md)](direct-consumer/README.md)
![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--consumer-00BC8E?labelColor=grey&style=flat)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-consumer/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-consumer)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer)


# Dependencies
### Gradle

The libraries are available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

```groovy
dependencies {
    implementation 'io.synadia:{artifact}:{major.minor.patch}'
}
```

If you need the before it propagates to Maven central, you can use the Sonatype repository:

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://oss.sonatype.org/content/repositories/releases"
    }
}
```

If you need a snapshot version, you must add the url for the snapshots.

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://s01.oss.sonatype.org/content/repositories/snapshots/"
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

If you need the before it propagates to Maven central, you can use the Sonatype repository:

```xml
<repositories>
    <repository>
        <id>sonatype releases</id>
        <url>https://oss.sonatype.org/content/repositories/releases</url>
        <releases>
           <enabled>true</enabled>
        </releases>
    </repository>
</repositories>
```

If you need a snapshot version, you must enable snapshots and change your dependency.

```xml
<repositories>
    <repository>
        <id>sonatype snapshots</id>
        <url>https://s01.oss.sonatype.org/content/repositories/snapshots/</url>
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
