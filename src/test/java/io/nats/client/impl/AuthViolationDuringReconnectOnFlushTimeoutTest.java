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

/* Test to reproduce #1426 */
public class AuthViolationDuringReconnectOnFlushTimeoutTest {

    private static void startServer(Context ctx) throws IOException {
        ctx.server.set(new NatsTestServer(new String[]{"--auth", "1234"}, ctx.port, false));
    }

    private static Runnable waitCloseSocket(Context ctx) {
        return () -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (!ctx.violated.get() && ctx.restartsLeft.get() >= 0) {
                if (ctx.nc.closeSocketLock.isLocked()) {
                    ctx.latch.countDown();
                    // just acquire the lock and release it
                    try {
                        ctx.nc.closeSocketLock.lock();
                    } finally {
                        ctx.nc.closeSocketLock.unlock();
                    }
                }
            }
        };
    }

    private static void restartServer(Context ctx) {
        try {
            ctx.restartsLeft.decrementAndGet();
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
                    .bufferSize(10)
                    .dataPortType(SlowSocketDataPort.class.getCanonicalName())
                    .reconnectBufferSize(10)
                    .reconnectWait(Duration.ofMillis(1000))
                    .connectionListener((conn, e) ->
                            System.out.println(String.format("Tid: %d, NATS: connection event - %s, connected url: %s. servers: %s ", Thread.currentThread().getId(), e, conn.getConnectedUrl(), conn.getServers())
                            ))
                    .errorListener(ctx.errorListener)
                    .build();

            ctx.nc = new MockPausingNatsConnection(options);
            ctx.nc.connect(true);
            ctx.d = ctx.nc.createDispatcher(m -> {
            });

            ctx.latch = new CountDownLatch(1);
            for (int i = 0; i < 1_000; i++) {
                String subject = "test_" + i;
                ctx.subscriptions.add(subject);
                ctx.d.subscribe(subject);
            }

            ctx.serverRestarter.scheduleWithFixedDelay(() -> restartServer(ctx), 2000, 5000, TimeUnit.MILLISECONDS);

            Thread t = new Thread(waitCloseSocket(ctx));
            t.start();
            t.join();

            assertFalse(ctx.violated.get());
        }
    }

    static class Context implements AutoCloseable {
        int port;
        NatsConnection nc;
        Dispatcher d;
        CountDownLatch latch;
        ConcurrentHashMap.KeySetView<String, Boolean> subscriptions = ConcurrentHashMap.newKeySet();
        ScheduledExecutorService serverRestarter = Executors.newSingleThreadScheduledExecutor();
        AtomicReference<NatsTestServer> server = new AtomicReference<>();
        AtomicBoolean violated = new AtomicBoolean(false);
        AtomicInteger restartsLeft = new AtomicInteger(3);
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

    static class MockPausingNatsConnection extends NatsConnection {
        private boolean reconnectFlow = false;

        MockPausingNatsConnection(@NonNull Options options) {
            super(options);
        }

        @Override
        void handleCommunicationIssue(Exception io)  {
            reconnectFlow = true;
            super.handleCommunicationIssue(io);
        }

        @Override
        void tryToConnect(NatsUri cur, NatsUri resolved, long now) {
            super.tryToConnect(cur, resolved, now);
            if (reconnectFlow && isConnected()) {
                SlowSocketDataPort.enabledPause.set(true);
            }
        }

        @Override
        public void flush(@Nullable Duration timeout) throws TimeoutException, InterruptedException {
            try {
                super.flush(timeout);
            } finally {
                reconnectFlow = false;
                SlowSocketDataPort.enabledPause.set(false);
            }
        }
    }

}
