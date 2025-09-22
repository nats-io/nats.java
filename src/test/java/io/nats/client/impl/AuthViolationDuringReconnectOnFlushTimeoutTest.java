package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/* Test to reproduce #1426 */
public class AuthViolationDuringReconnectOnFlushTimeoutTest {

    private static final int NUMBER_OF_SUBS = 1_000_000;

    private static void startServer(Context ctx) throws IOException {
        ctx.server.set(new NatsTestServer(new String[]{"--auth", "1234"}, ctx.port, false));
    }

    private static void restartServer(Context ctx) {
        try {
            if (ctx.restartsLeft.getCount() == 0) {
                return;
            }
            ctx.restartsLeft.countDown();
            ctx.server.get().shutdown();
            startServer(ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAuthViolationDuringReconnect() throws Exception {
        try (Context ctx = new Context()) {
            ctx.port = NatsTestServer.nextPort();
            startServer(ctx);

            Options options = new Options.Builder()
                    .servers(new String[]{"nats://" + "127.0.0.1:" + ctx.port})
                    .noRandomize()
                    .token(new char[]{'1', '2', '3', '4'})

                    .maxMessagesInOutgoingQueue(NUMBER_OF_SUBS )
                    .reconnectBufferSize(NUMBER_OF_SUBS * 100)
                    .connectionTimeout(Duration.ofMillis(10))
                    .reconnectWait(Duration.ofMillis(2000))
                    .connectionListener((conn, e) ->
                            System.out.println(String.format("Tid: %d, NATS: connection event - %s, connected url: %s. servers: %s ", Thread.currentThread().getId(), e, conn.getConnectedUrl(), conn.getServers())
                            ))
                    .errorListener(ctx.errorListener)
                    .build();

            ctx.nc = new NatsConnection(options);
            ctx.nc.connect(true);
            CountDownLatch processedMessages = new CountDownLatch(NUMBER_OF_SUBS);
            ctx.d = ctx.nc.createDispatcher();

            for (int i = 0; i < NUMBER_OF_SUBS; i++) {
                String subject = "test_" + i;
                ctx.subscriptions.add(subject);
                ctx.d.subscribe(subject, "q", m -> processedMessages.countDown());
            }

            ctx.serverRestarter.scheduleWithFixedDelay(() -> restartServer(ctx), 2, 20, TimeUnit.SECONDS);

            ctx.restartsLeft.await();
            TimeUnit.SECONDS.sleep(20); // give time to restore all subscriptions

            synchronized(ctx.nc.getStatus()) {
                while (ctx.nc.getStatus() != Connection.Status.CONNECTED && ctx.nc.getStatus() != Connection.Status.CLOSED) {
                }
            }
            assertFalse(ctx.violated.get());


            ctx.subscriptions.forEach(s -> ctx.nc.publish(s, "1".getBytes()));

            assertTrue(processedMessages.await(10, TimeUnit.SECONDS), "cdl " + processedMessages.getCount());
        }
    }

    static class Context implements AutoCloseable {
        int port;
        NatsConnection nc;
        Dispatcher d;
        ConcurrentHashMap.KeySetView<String, Boolean> subscriptions = ConcurrentHashMap.newKeySet();
        ScheduledExecutorService serverRestarter = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<NatsTestServer> server = new AtomicReference<>();
        AtomicBoolean violated = new AtomicBoolean(false);
        CountDownLatch restartsLeft = new CountDownLatch(2);
        ErrorListener errorListener = new ErrorListener() {
            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                System.out.println(String.format("Tid: %d, %s: Slow Consumer", Thread.currentThread().getId(), conn.getConnectedUrl()));
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                exp.printStackTrace();
                System.out.println(String.format("Tid: %d, Nats '%s' exception: %s", Thread.currentThread().getId(), conn.getConnectedUrl(), exp.toString()));
            }

            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.println(String.format("Tid: %d, Nats '%s': Error %s", Thread.currentThread().getId(), conn.getConnectedUrl(), error.toString()));

                if (error.contains("Authorization Violation")) {
                    violated.set(true);
                }
            }
        };

        @Override
        public void close() throws Exception {
            serverRestarter.shutdown();
            server.get().shutdown();
        }
    }

}
