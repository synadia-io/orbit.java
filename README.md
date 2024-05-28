<p align="center">
  <img src="orbit.png" alt="Orbit">
</p>

Orbit.java is a set of independent utilities or extensions around the [JNATS](https://github.com/nats-io/nats.java) ecosystem that aims to
boost productivity and provide higher abstraction layer for the [JNATS](https://github.com/nats-io/nats.java)
client. Note that these libraries will evolve rapidly and API guarantees are
not made until the specific project has a v1.0.0 version.

# Utilities

This is a list of the current utilities hosted here

| Module                | Description                            | Artifact                      | Docs                                         |
|-----------------------|----------------------------------------|-------------------------------|----------------------------------------------|
| Retrier               | Extension for retrying anything.       | `retrier`                     | [README.md](retrier/README.md)               |
| JS Publish Extensions | Extensions around Jetstream Publishing | `jnats-js-publish-extensions` | [README.md](js-publish-extensions/README.md) |


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

The libraries are available on the Maven central repository, and can be imported as a normal dependency in your pom.xml file:

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
