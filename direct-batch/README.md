![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Direct batch

Direct batch is beta.
It only works with the 2.11.x NATS Server and the JNats 2.20.5.main-2-11-SNAPSHOT

The direct batch functionality leverages the direct message capabilities introduced in NATS Server 2.11
The functionality is described in [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md) 

**Current Release**: 0.1.0 &nbsp; **Current Snapshot**: 0.1.1-SNAPSHOT

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--consumer-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-consumer)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-consumer/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-consumer)

## Message Info
When using direct api, the server returns `MessageInfo` instead of messages. 
The `MessageInfo` public api exposes the following state that is useful when processing the
result of direct calls.

```java
public boolean isMessage()     // represents an actual stream message
public boolean isStatus()      // any type of status message 
public boolean isEobStatus()   // a status message that represent the end of data has been reached
public boolean isErrorStatus() // a status message that represents an error
```

## Operation

1\. Create a DirectBatch instance. There are 2 constructors.
`JetStreamOptions` are not required except when working against a JetStream domain for instance.
The stream name is required to verify that it allows direct and then also during the underlying api calls.

```java
public DirectBatch(Connection conn, String streamName) throws IOException, JetStreamApiException
public DirectBatch(Connection conn, JetStreamOptions jso, String streamName) throws IOException, JetStreamApiException
```

2\. Create a MessageBatchGetRequest instance and call one of the 3 variants that do the actual request and return results.

### Fetch
```java
/**
 * Request a batch of messages using a {@link MessageBatchGetRequest}.
 * This ia a blocking call that returns when the entire batch has been satisfied.
 * <p>
 * @param messageBatchGetRequest the request details
 * @return a list containing {@link MessageInfo}
 */
public List<MessageInfo> fetchMessageBatch(MessageBatchGetRequest messageBatchGetRequest) {
```

### Queue
```java
/**
 * Request a batch of messages using a {@link MessageBatchGetRequest}.
 * This call is non-blocking and run's on the Connection Option's executor.
 * All MessageInfo's will be added to the queue.
 * <p>
 * @param messageBatchGetRequest the request details
 * @return a queue used to asynchronously receive {@link MessageInfo}
 */
public LinkedBlockingQueue<MessageInfo> queueMessageBatch(MessageBatchGetRequest messageBatchGetRequest)
```

### Request
```java
/**
 * Request a batch of messages using a {@link MessageBatchGetRequest}.
 * This call is a blocking call that returns true if the operation ended without an error status
 * or false if it did. It's mostly a redundant flag since the error will always be given to the handler.
 * <p>
 * Since it's a blocking call, either the caller or the handler needs to run on a different thread.
 * The queueMessageBatch implementation uses this under the covers and can be looked at as an example
 * <p>
 * This is an advanced api. The main caveat is that the handler is called in a blocking fashion. 
 * A RuntimeException produced by the handler allowed to propagate.  
 * <p>
 * @param messageBatchGetRequest the request details
 * @param handler                the handler used for receiving {@link MessageInfo}
 * @return true if all messages were received and properly terminated with a server EOB
 */
public boolean requestMessageBatch(MessageBatchGetRequest messageBatchGetRequest, MessageInfoHandler handler)
```

## MessageInfoHandler

The MessageInfoHandler is a simple callback interface used to receive messages from the `requestMessageBatch` api call.   
```java
void onMessageInfo(MessageInfo messageInfo);
```

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
