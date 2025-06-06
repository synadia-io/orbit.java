This document is the draft design describing a managed async publish utility.

## Publisher

#### Required Properties

* JetStream context on which to publish

#### Optional Properties
| Property                             | Description                                                                                                                                                                                                                                                                  |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| String idPrefix                      | used to make unique identifiers around each message. Defaults to a NUID                                                                                                                                                                                                      |
| int maxInFlight                      | no more than this number of messages can be waiting for publish ack. Defaults to 50.                                                                                                                                                                                         |
| int refillAllowedAt                  | if the queue size reaches maxInFlight, a hold is placed so no more messages can be published until the in flight queue contains this amount or less messages, at which time the hold is removed. Defaults to 0 which would be full sawtooth. Non zero provides for a window. |
| RetryConfig retryConfig              | if the user wants to publish with retries, they must supply a config, otherwise the publish will be attempted only once.                                                                                                                                                     |
| long pollTime                        | the amount of time in ms to poll any given queue. Ensures polling doesn't block indefinitely. Defaults to 100ms                                                                                                                                                              |
| long publishPauseTime                | the amount of time in ms to pause between checks when hold is on. Defaults to 100ms                                                                                                                                                                                          |
| long waitTimeout                     | the timeout when waiting for a publish to be acknowledged. Defaults to 5000ms                                                                                                                                                                                                |
| PublisherListener publisherListener  | a callback for the user to see what's going on in the workflow, see description of Flight later                                                                                                                                                                              |


## PublisherListener Interface

The callback interface for the user to get information about the publish workflow

| Method                                                                | Description                                                           |
|-----------------------------------------------------------------------|-----------------------------------------------------------------------|
| void published(Flight flight)                                         | the flight is ready when the message is published                     |
| void acked(Flight flight)                                             | the publish ack was received                                          |
| void completedExceptionally(Flight flight)                            | the publish exceptioned, such as a 503 or lower level request timeout |
| void timeout(Flight flight)                                           | the ack was not returned in time based on waitTimeout                 |
| void paused(int currentInFlight, int maxInFlight, int resumeAmount)   | Publishing was paused due to in-flight conditions                     |
| void resumed(int currentInFlight, int maxInFlight, int resumeAmount)  | Publishing was resumed due to in-flight conditions                    |

    /**
     * The engine has just resumed publishing and will continue unless
     * the number of messages in flight reaches the max
     * @param currentInFlight the number of messages in flight
     * @param maxInFlight the number of in flight messages when publishing will be paused
     * @param resumeAmount the number of in flight messages when publishing will resume after being paused
     */
    void resumed(int currentInFlight, int maxInFlight, int resumeAmount);

## Flight structure

An object that holds state of the message as it makes its way through the workflow

| Property                                            | Description                                                                          |
|-----------------------------------------------------|--------------------------------------------------------------------------------------|
| long getPublishTime()                               | the time when the actual js publish was executed. Used for waitTimeout determination |
| CompletableFuture<PublishAck> getPublishAckFuture() | the future for the publish ack                                                       |
| String getId()                                      | `<idPrefix>-<number>` for example                                                    |
| String getSubject()                                 | the original message subject                                                         |
| Headers getHeaders()                                | the original message headers                                                         |
| byte[] getBody()                                    | the original message body                                                            |
| PublishOptions getOptions()                         | the original message publish options (i.e. expectations)                             |

## Behavior

| Name          | Description                                                                                                                                                                                          |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| publishRunner | loop that reads from the queue of messages added by the user                                                                                                                                         |
| flightsRunner | loop that checks the status of publish acks                                                                                                                                                          |
| start         | start the runners with individual threads. Designed to allow user to start the runners on threads they provide themselves.                                                                           |
| stop          | stop the runners before the next iteration                                                                                                                                                           |
| drain         | prevent any messages being added to the queue. Workflow stops when all messages are published then acknowledged/exceptioned/time-out                                                                 |
| publishAsync  | parallel api to JetStream publishAsync api. All return a `CompletableFuture<Flight>` that indicates the message is actually published. Can be ignored in place of PublisherListener.publish callback |

## Publish Runner Pseudo Code
```
while keepGoing flag
  if not in holding pattern
    check the user's queue using pollTime
    if there is something...
      if it's the drain marker, we are done
      else
        publish async (with retry config if provided)
        put message in in flight queue
        if in flight queue has reached maxInFlight put hold on
        notify listener to indicate published
  else in holding pattern
    sleep publishPauseTime
```

## Flights Runner Pseudo Code:
```
while keepGoing flag
  check the in flight queue using pollTime
    if there is something
      if been asked to drain and user queue is empty and in flights queue is empty, we are done
        else
          if the flight's publish ack future is done
            if the publish ack was received
              notify listener to indicate the publish was acked
            else if the future was completed with an exception
              notify listener to indicate the publish completed exceptionally
            else if the wait timeout has been exceeded
              notify listener to indicate the publish timed out
              complete the future exceptionally so if the user is waiting on the future instaed of the callback they can see it
          else if otherwise not done, put it back in the queue for later
      if in flight queue has equal to or less than the refillAllowedAt threshold make remove the hold
```