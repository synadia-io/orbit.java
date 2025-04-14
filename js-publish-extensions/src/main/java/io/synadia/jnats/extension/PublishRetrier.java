// Copyright (c) 2024-2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.jnats.extension;

import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static io.synadia.jnats.extension.PublishRetryConfig.DEFAULT_CONFIG;
import static io.synadia.retrier.Retrier.execute;

/**
 * The Publish Retrier provides methods which are built specifically for JetStream publishing.
 */
public class PublishRetrier {

    private static final String NO_RESPONDERS_TEXT = "No Responders";

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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return execute(config.retryConfig,
            () -> js.publish(subject, headers, body, options), e -> {
                if (config.retryAll) {
                    return true;
                }
                if (e instanceof IOException) {
                    // No responders are actually surfaced as an IOException
                    // but we are treating it as its own entity,
                    // meaning no-responders does not follow the retryOnIoEx flag
                    // and we check it first.
                    if (e.getMessage().contains(NO_RESPONDERS_TEXT)) {
                        return config.retryOnNoResponders;
                    }
                    if (config.retryOnIoEx) {
                        return true;
                    }
                }
                if (config.retryOnJetStreamApiEx && e instanceof JetStreamApiException) {
                    return true;
                }
                return config.retryOnRuntimeEx && e instanceof RuntimeException;
            });
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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, String subject, byte[] body) throws Exception {
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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, String subject, Headers headers, byte[] body) throws Exception {
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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, Message message) throws Exception {
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
    public static PublishAck publish(PublishRetryConfig config, JetStream js, Message message, PublishOptions options) throws Exception {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, String subject, byte[] body) {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, String subject, Headers headers, byte[] body) {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, Message message) {
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
    public static CompletableFuture<PublishAck> publishAsync(PublishRetryConfig config, JetStream js, Message message, PublishOptions options) {
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
