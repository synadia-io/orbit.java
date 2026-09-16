<img src="../orbit_shorter.png" alt="Orbit">

# Batch Publish

Publish a group of messages as one unit. [ADR-50](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md) defines two ways to do this, and they have opposite goals. They share a vocabulary — a batch id, a batch sequence, and a `PublishAck` carrying `batch` and `count` — but they are separate wire protocols, so pick the one that matches what you need.

| | Atomic Batch Publish | Fast-Ingest Publish |
|---|---|---|
| Guarantee | All messages stored, or none | None; messages are stored as they arrive |
| Size limit | 1000 messages | No limit |
| Messages can be lost | No | Yes, and you choose how that is handled |
| Flow control | No | Yes, server driven |
| Stream config | `allow_atomic` | `allow_batched` |
| Server | 2.12.0+ | 2.14.0+ |
| Java types | `BatchPublisher`, `EobBatchPublisher` | `FastPublisher`, `EobFastPublisher` |

## Atomic Batch Publish

A group of up to 1000 messages that all get added to the stream or none do.

* Messages are staged in memory on the server until the commit; nothing is stored before then.
* This is about transactions, not speed.

### Ending a batch

There are two ways to end an atomic batch, one per type. Both add messages identically — same `add`/`addAcked`, same headers, same options.

`BatchPublisher.commit(subject, data)` sends a final real message and stores it along with the rest of the batch. Use it when the last thing you have to publish is genuinely the last message of your transaction.

`EobBatchPublisher.commit()` takes no message and ends the batch **without storing one**. The server discards the sentinel's payload, rewrites the header of the previously received last message so the batch commits normally, and reports a batch size that excludes the sentinel. Use it when your transaction is exactly the messages you already published — otherwise you would have to hold one message back just to carry the commit, or invent a filler message and permanently store a piece of junk in the stream.

```java
EobBatchPublisher bp = EobBatchPublisher.builder().connection(nc).build();
bp.add(subject, data);
bp.add(subject, data);
PublishAck pa = bp.commit(); // no message, no subject; pa.getBatchSize() is 2
```

The sentinel is published on the subject of the **first** message added.

A batch cannot consist of only a sentinel. `size()` always reports the number of *stored* messages, so it agrees with `PublishAck.getBatchSize()` in both cases, and the publisher checks that agreement on the ack rather than assuming it.

### What the publisher refuses

Four things are rejected locally rather than sent for the server to reject, because each of them costs the whole batch:

* An expected **last sequence** on any message but the first. ADR-50 allows it only on the first message, and the server rejects the entire batch when a later one carries it. The expected last *subject* sequence and the expected stream are not restricted this way and can go on any message.
* The batch protocol headers — `Nats-Batch-Id`, `Nats-Batch-Sequence`, `Nats-Batch-Commit` — in your own headers. The publisher writes those itself.
* `Nats-Expected-Last-Msg-Id`, which the server refuses inside a batch. `Nats-Msg-Id` is fine: batch de-duplication is supported from server 2.12.1.
* A batch id that is not a single subject token, or is longer than 64 characters. Fast ingest carries the id in the reply subject, so a dot in it would silently become a different batch id on the server.

**`EobBatchPublisher` requires a server at 2.14.0 or later**, checked at `build()`. `BatchPublisher` needs only 2.12.0.

## Fast-Ingest Publish

**This is not atomic.** There is no staging and no all-or-nothing guarantee: messages are persisted as they arrive, the batch has no size limit, and messages can be dropped by the server's overload protection or lost across a stream leader change. What you get in exchange is a control channel over which the server continuously tells you how fast you are allowed to go, which is what keeps many concurrent producers from burying a stream.

Because messages can be lost, you choose up front what a gap means to you:

| `GapMode` | Behavior | Use when |
|---|---|---|
| `Fail` (default) | Any gap abandons the batch. The server stops accepting messages and sends a final `PublishAck` reporting how far it got. | A gap is a hole in your data — the ObjectStore-shaped case. |
| `Ok` | Gaps are reported to your listener and the batch continues from the received sequence. | You are shipping a firehose where a lost message is survivable, such as metrics. |

The same choice governs per-message header check failures such as `Nats-Expected-Last-Sequence`: in `Fail` they stop the batch, in `Ok` they are reported and the batch continues.

Fast ingest ends the same two ways as an atomic batch, and uses the same two types of publisher:

```java
EobFastPublisher fp = EobFastPublisher.builder()
    .connection(nc)
    .gapMode(GapMode.Fail)
    .maxFlow(100)               // most messages the server may go between acks
    .maxOutstandingAcks(2)      // how far ahead you are willing to run
    .listener(myListener)
    .build();                   // local only, no server contact yet

fp.add(subject, data);          // blocks only when flow control says so
PublishAck pa = fp.commit();    // ends the batch, stores no final message
```

Use `FastPublisher` instead when you do want the message that ends the batch stored; it ends with `commit(subject, data)`. Everything else — `add`, `ping`, `abandon`, flow control, gap handling — is identical and shared.

Notes:

* `build()` does not contact the server. Feature detection happens on the first `add`.
* The fast publishers are **not thread safe** and should be owned by one producer thread. Multiple concurrent producers should each hold their own.
* The control channel is read on a dispatcher of the publisher's own, so a batch the server abandons is known to be over immediately — `isTerminal()` and `getEndReason()` are current even while the application is between publishes, with no `ping()` needed for that.
* Listener callbacks still run on the thread that called `add`, `commit` or `ping`, in arrival order, and so do the counters. That thread does the accounting; the dispatcher only classifies. Call `ping()` if you want the callbacks and the flow state brought up to date without publishing.
* `abandon()` gives up without committing, and `close()` is the same thing, so try-with-resources releases the control channel — the dispatcher and its thread — without ever committing a batch whose assembly threw. The atomic publishers hold no such resource and use `discard()`.
* `getEndReason()` says why a batch ended — `Open`, `Committed`, `Gap`, `Error` or `Abandoned` — which `isTerminal()` alone cannot.
* When a gap or a per message error ends a `Fail` batch, the server abandons it and sends a final `PublishAck` saying how far it actually got. Committing such a batch does not publish anything; it throws, carrying that ack: `catch (FastPublishException e) { e.getPublishAck(); }`. It is the only authoritative statement of what was stored — a gap report explicitly is not — and it may be absent, since these acks are best effort.
* `ping()` goes to the subject of the first message in the batch, and a batch with no messages cannot be pinged.

## Changes in 0.3.0

Fast-ingest publishing is new in this release, including a control channel read asynchronously on the publisher's own dispatcher: `FastPublisher`, `EobFastPublisher`, `GapMode`, `FastPublishListener` and the `FastFlowGap` / `FastFlowError` / `FastPubAck` reports. `EobBatchPublisher` is new too, so atomic batches can now end without storing a message.

Changes to what was already there:

* **Ack timeouts are milliseconds.** `ackTimeout(long millis)` on both builders; below 1 means the default. The `Duration` overload still compiles and converts, and is deprecated. A `Duration` under a millisecond used to mean *wait forever* on one path and *time out immediately* on the other.
* **The per message ack settings are gone from `BatchPublishOptions`.** `ackTimeout`, `ackFirst` and `ackEvery` were accepted there and never read. They belong to the batch, not to one message, and they have always worked on the publisher's builder.
* **A connection level rejection is now a `BatchPublishException`.** An invalid subject, a closed or draining connection, or a full reconnect buffer used to escape `add` as an unchecked exception with no batch id attached.
* **An `add` that is acknowledged now reports the server's error.** It used to say only "Invalid ack returned from add with confirm", dropping the reason — `atomic publish is disabled`, for instance — on the floor.
* **`commitAsync` no longer buries the cause.** `ExecutionException.getCause()` is now the `BatchPublishException` itself rather than a `RuntimeException` wrapping it.
* **A publisher level message TTL now applies to every message.** It was silently ignored unless that message also carried a `BatchPublishOptions`.
* The publisher rejects the four things listed under [What the publisher refuses](#what-the-publisher-refuses), and validates the commit's `PublishAck` against its own count and batch id.

![Artifact](https://img.shields.io/badge/Artifact-io.synadia:batch--publish-197556?labelColor=grey&style=flat)
![0.2.2](https://img.shields.io/badge/Current_Release-0.2.2-27AAE0)
![0.3.0](https://img.shields.io/badge/Current_Snapshot-0.3.0--SNAPSHOT-27AAE0)
[![Dependencies Help](https://img.shields.io/badge/Dependencies%20Help-27AAE0)](https://github.com/synadia-io/orbit.java?tab=readme-ov-file#dependencies)
[![javadoc](https://javadoc.io/badge2/io.synadia/batch-publish/javadoc.svg)](https://javadoc.io/doc/io.synadia/batch-publish)
[![Maven Central](https://img.shields.io/maven-central/v/io.synadia/batch-publish)](https://img.shields.io/maven-central/v/io.synadia/batch-publish)


---
Copyright (c) 2024-2026 Synadia Communications Inc. All Rights Reserved.
See [LICENSE](LICENSE) and [NOTICE](NOTICE) file for details.
