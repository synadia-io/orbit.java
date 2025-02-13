![Synadia](src/main/javadoc/images/synadia-logo.png) &nbsp;&nbsp;&nbsp;&nbsp; ![NATS](src/main/javadoc/images/large-logo.png)

# Direct batch

Direct batch is beta.
It only works with the 2.11.x NATS Server and the JNats 2.20.5.main-2-11-SNAPSHOT

The direct batch functionality leverages the direct message capabilities introduced in NATS Server 2.11
The functionality is described in [ADR-31](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-31.md) 

**Current Release**: 0.1.0
&nbsp;**Current Snapshot**: 0.1.1-SNAPSHOT
&nbsp; **Gradle and Maven** `io.synadia:direct-batch`
[Dependencies Help](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies) 

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:direct--batch-00BC8E?labelColor=grey&style=flat)
[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-batch/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.synadia/direct-batch)
[![javadoc](https://javadoc.io/badge2/io.synadia/direct-batch/javadoc.svg)](https://javadoc.io/doc/io.synadia/direct-batch)


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

## Basic Operation

1\. Create a DirectBatchContext instance. There are 2 constructors.
`JetStreamOptions` are not required except when working against a JetStream domain for instance.
The stream name is required to verify that it allows direct and then also during the underlying api calls.

```java
public DirectBatchContext(Connection conn, String streamName) throws IOException, JetStreamApiException
public DirectBatchContext(Connection conn, JetStreamOptions jso, String streamName) throws IOException, JetStreamApiException
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
public List<MessageInfo> fetchMessageBatch(MessageBatchGetRequest messageBatchGetRequest)
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

## MessageBatchGetRequest

The `MessageBatchGetRequest` is designed to simplify use of the server's direct batch support,
and has static methods that return an instance.

---
1\. Get up to batch number of messages where the message sequence is >= 1 and for the specified subject
```java
batch(String subject, int batch)
```

---
2\. Get up to batch number of messages where the message sequence is >= the specified sequence and for the specified subject
```java
batch(String subject, int batch, long minSequence)
```

---
3\. Get up to batch number of messages where the message timestamp is >= than start time and for the specified subject
```java
batch(String subject, int batch, ZonedDateTime startTime)
```

---
4\. Get up to batch number of messages where the message sequence is >= 1, for the specified subject, and limited by max bytes
```java
batchBytes(String subject, int batch, int maxBytes)
```

---
5\. Get up to batch number of messages where the message sequence is >= than the specified sequence, for the specified subject and limited by max bytes
```java
batchBytes(String subject, int batch, int maxBytes, long minSequence)
```

---
6\. Get up to batch number of messages where the message timestamp is >= than start time, for the specified subject and limited by max bytes
```java
batchBytes(String subject, int batch, int maxBytes, ZonedDateTime startTime)
```

---
7\. Get the last messages for the subjects specified subject
```java
multiLastForSubjects(List<String> subjects)
```

---
8\. Get the last messages for the subjects, where the last message is less than or equal to the up to sequence.
```java
multiLastForSubjects(List<String> subjects, long upToSequence)
```

---
9\. Get the last messages for the subjects, where the last message is less than or equal to the up to time.
```java
multiLastForSubjects(List<String> subjects, ZonedDateTime upToTime)
```

---
10\. Get the last messages for the subjects specified subject, limited by batch size
```java
multiLastForSubjectsBatch(List<String> subjects, int batch)
```

---
11\. Get the last messages for the subjects, where the last message is less than or equal to the up to sequence, limited by batch size.
```java
multiLastForSubjectsBatch(List<String> subjects, long upToSequence, int batch)
```

---
12\. Get the last messages for the subjects, where the last message is less than or equal to the up to time, limited by batch size.
```java
multiLastForSubjectsBatch(List<String> subjects, ZonedDateTime upToTime, int batch)
```

## Examples

The [RequestMessageBatchExamples.java](src/examples/java/io/synadia/examples/RequestMessageBatchExamples.java)
demonstrate usage and behavior when using the `requestMessageBatch` DirectBatchContext api call.

The [QueueMessageBatchExamples.java](src/examples/java/io/synadia/examples/QueueMessageBatchExamples.java)
demonstrate usage and behavior when using the `queueMessageBatch` DirectBatchContext api call.

The [FetchMessageBatchExamples.java](src/examples/java/io/synadia/examples/FetchMessageBatchExamples.java)
demonstrate usage and behavior when using the `fetchMessageBatch` DirectBatchContext api call.

The [ErrorExamples.java](src/examples/java/io/synadia/examples/ErrorExamples.java) demonstrates errors...
1. Stream must have "allow direct" set.
2. Creating a `DirectBatchContext` object...
3. Creating a `MessageBatchGetRequest` object...

---
Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
