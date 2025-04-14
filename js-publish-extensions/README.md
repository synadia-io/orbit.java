![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# JNATS JetStream Publisher Extensions

Extensions specific to JetStream publishing.

**Current Release**: 0.3.0
&nbsp; **Current Snapshot**: 0.4.0-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:jnats-js-publish-extensions`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:jnats--js--publish--extensions-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/jnats-js-publish-extensions)
[![javadoc](https://javadoc.io/badge2/io.synadia/jnats-js-publish-extensions/javadoc.svg)](https://javadoc.io/doc/io.synadia/jnats-js-publish-extensions)

### PublishRetrier

This class parallels the standard JetStream publish api with methods that will retry the publish.

The examples:
* [Publish Retrier Sync Example](src/examples/java/io/synadia/examples/PublishRetrierSyncExample.java)
* [Publish Retrier Async Example](src/examples/java/io/synadia/examples/PublishRetrierAsyncExample.java)

### AsyncJsPublisher

This class is a full async message publish manager. 
This utility provides a workflow of
1. Publishing a message async
   * The number of inflight messages (published but not received acks) can be set.
2. Queueing and tracking of the in-flight PublishAck future
3. The ability to observe the queue and respond to events
   * The message was published
   * The message received a valid ack
   * The publish completed with an exception
   * The publish timed out.

It can be combined with the retrier. 
You must consider that when publishing async in this manner 
it's possible for messages to be published out of order.
In that case you can use publish expectations.
If order of messages is a requirement, you 

* [Async Js Publisher Example](src/examples/java/io/synadia/examples/AsyncJsPublisherExample.java)
* [Async Js Publisher More Customized Example](src/examples/java/io/synadia/examples/AsyncJsPublisherCustomizedExample.java)

---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
