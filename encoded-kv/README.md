![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Encoded Key Value

Encoded Key Value provides a way to use Key Value with encoded keys and values.

It is a Java Generic version of the Key Value interface and provides the ability
* to have something other than a string for a key.
* to have an object instead of a byte array as a value

It requires a _codec_, which 
* encodes the key object as a string
* encodes the value object as a byte array
* decodes the encoded key back to the key object
* decodes the encoded data bytes back into the value object.

**Current Release**: N/A
&nbsp; **Current Snapshot**: 0.1.0-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:encoded-kv`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--consumer-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-consumer/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-consumer)

The [Unit Tests](src/test/java/io/synadia/jnats/extension/DirectConsumerTests.java) may also be of interest.

---
Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
