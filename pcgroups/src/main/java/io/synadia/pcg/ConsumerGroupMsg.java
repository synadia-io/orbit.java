// Copyright 2024-2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.pcg;

import io.nats.client.Message;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.time.Duration;

/**
 * Wrapper for JetStream messages that strips the partition number from the subject.
 */
public class ConsumerGroupMsg {

    private final Message msg;

    public ConsumerGroupMsg(Message msg) {
        this.msg = msg;
    }

    /**
     * Returns the message body data.
     */
    public byte[] getData() {
        return msg.getData();
    }

    /**
     * Returns the message headers.
     */
    public Headers getHeaders() {
        return msg.getHeaders();
    }

    /**
     * Returns the subject with the partition number prefix stripped.
     * The original subject format is "{partitionNumber}.{originalSubject}".
     */
    public String getSubject() {
        String subject = msg.getSubject();
        if (subject == null) {
            return null;
        }
        int dotIndex = subject.indexOf('.');
        if (dotIndex >= 0 && dotIndex < subject.length() - 1) {
            return subject.substring(dotIndex + 1);
        }
        return subject;
    }

    /**
     * Returns the original subject including partition prefix.
     */
    public String getRawSubject() {
        return msg.getSubject();
    }

    /**
     * Returns the reply subject for the message.
     */
    public String getReplyTo() {
        return msg.getReplyTo();
    }

    /**
     * Returns the underlying message for metadata access.
     */
    public Message getMessage() {
        return msg;
    }

    /**
     * Acknowledges the message.
     * This tells the server that the message was successfully processed.
     */
    public void ack() throws IOException, InterruptedException {
        msg.ack();
    }

    /**
     * Acknowledges the message and waits for acknowledgment from server.
     */
    public void ackSync(Duration timeout) throws IOException, InterruptedException, java.util.concurrent.TimeoutException {
        msg.ackSync(timeout);
    }

    /**
     * Negatively acknowledges the message.
     * This tells the server to redeliver the message.
     */
    public void nak() throws IOException, InterruptedException {
        msg.nak();
    }

    /**
     * Negatively acknowledges the message with a delay.
     * This tells the server to redeliver the message after the specified delay.
     */
    public void nakWithDelay(Duration delay) throws IOException, InterruptedException {
        msg.nakWithDelay(delay);
    }

    /**
     * Tells the server that this message is being worked on.
     * Resets the redelivery timer on the server.
     */
    public void inProgress() throws IOException, InterruptedException {
        msg.inProgress();
    }

    /**
     * Tells the server to not redeliver this message, regardless of MaxDeliver setting.
     */
    public void term() throws IOException, InterruptedException {
        msg.term();
    }

    /**
     * Returns the pinned ID from message headers if present.
     */
    public String getPinnedId() {
        Headers headers = msg.getHeaders();
        if (headers != null) {
            return headers.getFirst("Nats-Pin-Id");
        }
        return null;
    }
}
