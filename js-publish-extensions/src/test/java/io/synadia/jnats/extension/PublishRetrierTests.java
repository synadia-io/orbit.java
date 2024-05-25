package io.synadia.jnats.extension;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import nats.io.ConsoleOutput;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static io.synadia.retrier.RetryConfig.DEFAULT_CONFIG;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PublishRetrierTests {
    static {
        NatsServerRunner.setDefaultOutputSupplier(ConsoleOutput::new);
        NatsServerRunner.setDefaultOutputLevel(Level.WARNING);
    }

    interface SyncRetryFunction {
        PublishAck execute(String subject) throws Exception;
    }

    interface AsyncRetryFunction {
        CompletableFuture<PublishAck> execute(String subject) throws Exception;
    }

    @Test
    public void testRetryJsApis() throws Exception {
        try (NatsServerRunner runner = new NatsServerRunner(false, true)) {
            try (Connection nc = Nats.connect(runner.getURI())) {
                final JetStream js = nc.jetStream();

                _testRetrySync(nc, subject -> PublishRetrier.publish(js, subject, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js,  subject, null, null, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js,  subject, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js,  subject, (Headers)null, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js,  subject, null, (PublishOptions) null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js, message(subject)));
                _testRetrySync(nc, subject -> PublishRetrier.publish(js, message(subject), null));

                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js, subject, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js,  subject, null, null, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js,  subject, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js,  subject, (Headers)null, null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js,  subject, null, (PublishOptions)null));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js, message(subject)));
                _testRetrySync(nc, subject -> PublishRetrier.publish(DEFAULT_CONFIG, js, message(subject), null));

                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, subject, null, null, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, subject, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, subject, (Headers)null, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, subject, null, (PublishOptions)null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, message(subject)));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(js, message(subject), null));

                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, subject, null, null, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, subject, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, subject, (Headers)null, null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, subject, null, (PublishOptions)null));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, message(subject)));
                _testRetryAsync(nc, subject -> PublishRetrier.publishAsync(DEFAULT_CONFIG, js, message(subject), null));
            }
        }
    }

    private Message message(String subject) {
        return NatsMessage.builder().subject(subject).build();
    }

    private static void _testRetrySync(Connection nc, SyncRetryFunction f) throws Exception {
        String stream = unique();
        String subject = unique();
        new Thread(() -> {
            try {
                Thread.sleep(300);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build());
            }
            catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        }).start();

        assertNotNull(f.execute(subject));
    }

    private static void _testRetryAsync(Connection nc, AsyncRetryFunction f) throws Exception {
        String stream = unique();
        String subject = unique();
        new Thread(() -> {
            try {
                Thread.sleep(300);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                    .name(stream)
                    .subjects(subject)
                    .storageType(StorageType.Memory)
                    .build());
            }
            catch (IOException | JetStreamApiException e) {
                throw new RuntimeException(e);
            }
        }).start();

        CompletableFuture<PublishAck> fpa = f.execute(subject);
        assertNotNull(fpa.get(2, TimeUnit.SECONDS));
    }

    private static String unique() {
        return NUID.nextGlobalSequence();
    }
}
