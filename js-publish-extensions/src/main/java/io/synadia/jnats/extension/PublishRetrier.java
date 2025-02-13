// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.Status;
import io.synadia.retrier.RetryConfig;

import java.util.concurrent.CompletableFuture;

import static io.synadia.retrier.Retrier.execute;
import static io.synadia.retrier.RetryConfig.DEFAULT_CONFIG;

/**
 * The Publish Retrier provides methods which are built specifically for JetStream publishing.
 */
public class PublishRetrier {
    private PublishRetrier() {}  /* ensures cannot be constructed */

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return execute(config,
            () -> js.publish(subject, headers, body, options),
            e -> e.getMessage().contains(Status.NO_RESPONDERS_TEXT));
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body) throws Exception {
        return publish(config, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(config, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(config, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, Message message) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, Message message) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(config, js, subject, headers, body, options);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, byte[] body) {
        return publishAsync(config, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body) {
        return publishAsync(config, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) {
        return publishAsync(config, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, Message message) {
        return publishAsync(config, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, Message message, PublishOptions options) {
        return publishAsync(config, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, subject, headers, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, byte[] body) {
        return publishAsync(DEFAULT_CONFIG, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, Headers headers, byte[] body) {
        return publishAsync(DEFAULT_CONFIG, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, byte[] body, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, Message message) {
        return publishAsync(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, Message message, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }
}
