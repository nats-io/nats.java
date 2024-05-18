package io.nats.client.utility;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.utility.RetryConfig.*;
import static org.junit.jupiter.api.Assertions.*;

public class RetrierTests extends JetStreamTestBase {

    @Test
    public void testRetryConfigBuilding() {
        RetryConfig rc = DEFAULT_CONFIG;
        assertEquals(DEFAULT_ATTEMPTS, rc.getAttempts());
        assertArrayEquals(DEFAULT_BACKOFF_POLICY, rc.getBackoffPolicy());
        assertEquals(Long.MAX_VALUE, rc.getDeadline());

        long[] backoffPolicy = new long[]{300};
        rc = RetryConfig.builder()
            .attempts(1)
            .backoffPolicy(backoffPolicy)
            .deadline(1000)
            .build();

        assertEquals(1, rc.getAttempts());
        assertArrayEquals(backoffPolicy, rc.getBackoffPolicy());
        assertEquals(1000, rc.getDeadline());

        rc = RetryConfig.builder()
            .attempts(0)
            .deadline(0)
            .build();

        assertEquals(DEFAULT_ATTEMPTS, rc.getAttempts());
        assertEquals(Long.MAX_VALUE, rc.getDeadline());
    }

    interface SyncRetryFunction {
        PublishAck execute(String subject) throws Exception;
    }

    interface AsyncRetryFunction {
        CompletableFuture<PublishAck> execute(String subject) throws Exception;
    }

    @Test
    public void testRetryExecute() throws Exception {
        AtomicInteger counterExhaustAttempts = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(DEFAULT_CONFIG,
                () -> { throw new Exception("Attempt: " + counterExhaustAttempts.incrementAndGet()); }));
        assertEquals(3, counterExhaustAttempts.get());

        AtomicInteger counterExhaustDeadline = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().attempts(Integer.MAX_VALUE).deadline(500).build(),
                () -> { throw new Exception("Attempt: " + counterExhaustDeadline.incrementAndGet()); }));
        assertTrue(counterExhaustDeadline.get() > 1);

        AtomicInteger counterExhaustObserver = new AtomicInteger();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().attempts(Integer.MAX_VALUE).build(),
                () -> { throw new Exception("Attempt: " + counterExhaustObserver.incrementAndGet()); },
                e -> counterExhaustObserver.get() < 5));
        assertEquals(5, counterExhaustObserver.get());

        long[] policy = new long[]{100};
        AtomicInteger counterMoreAttemptsThanPolicyEntries = new AtomicInteger();
        long start = System.currentTimeMillis();
        assertThrows(Exception.class,
            () -> Retrier.execute(RetryConfig.builder().backoffPolicy(policy).build(),
                () -> { throw new Exception("Attempt: " + counterMoreAttemptsThanPolicyEntries.incrementAndGet());}));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 300);
    }

    @Test
    public void testRetryJsApis() throws Exception {
        jsServer.run(nc -> {
            final JetStream js = nc.jetStream();

            _testRetrySync(nc, subject -> Retrier.publish(js, subject, null));
            _testRetrySync(nc, subject -> Retrier.publish(js,  subject, null, null, null));
            _testRetrySync(nc, subject -> Retrier.publish(js,  subject, null));
            _testRetrySync(nc, subject -> Retrier.publish(js,  subject, (Headers)null, null));
            _testRetrySync(nc, subject -> Retrier.publish(js,  subject, null, (PublishOptions) null));
            _testRetrySync(nc, subject -> Retrier.publish(js, new NatsMessage(subject, null, null)));
            _testRetrySync(nc, subject -> Retrier.publish(js, new NatsMessage(subject, null, null), null));

            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js, subject, null));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js,  subject, null, null, null));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js,  subject, null));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js,  subject, (Headers)null, null));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js,  subject, null, (PublishOptions)null));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js, new NatsMessage(subject, null, null)));
            _testRetrySync(nc, subject -> Retrier.publish(DEFAULT_CONFIG, js, new NatsMessage(subject, null, null), null));

            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, subject, null, null, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, subject, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, subject, (Headers)null, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, subject, null, (PublishOptions)null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, new NatsMessage(subject, null, null)));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(js, new NatsMessage(subject, null, null), null));

            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, subject, null, null, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, subject, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, subject, (Headers)null, null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, subject, null, (PublishOptions)null));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, new NatsMessage(subject, null, null)));
            _testRetryAsync(nc, subject -> Retrier.publishAsync(DEFAULT_CONFIG, js, new NatsMessage(subject, null, null), null));
        });
    }

    private static void _testRetrySync(Connection nc, SyncRetryFunction f) throws Exception {
        String stream = stream();
        String subject = subject();
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
        String stream = stream();
        String subject = subject();
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
}
