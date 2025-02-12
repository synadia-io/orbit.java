![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Direct batch

Direct batch is beta.
It only works with the 2.11.x NATS Server and the JNats 2.20.5.main-2-11-SNAPSHOT

The direct batch functionality leverages the direct message capabilities introduced in NATS Server 2.11
The functionality is described in [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md) 

**Current Release**: N/A &nbsp; **Current Snapshot**: 0.1.0-SNAPSHOT

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--consumer-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-consumer/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-consumer)

## Examples

### Error Examples

The [ErrorExamples.java](src/examples/java/io/synadia/examples/ErrorExamples.java) source
demonstrates cases around
1. Stream must have allow direct set.
2. Creating a `DirectBatch` object...
3. Creating a `MessageBatchGetRequest` object...

### Unit Tests
The [Unit Tests](src/test/java/io/synadia/jnats/extension/DirectBatchTests.java) may also be of interest.

### Gradle and Maven

See the [Main README.md](../README.md). The group is `io.synadia` The artifact is `direct-consume`

Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
