package io.nats.client.impl;

import io.nats.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;

/* Test to reproduce #1320 */
public class AuthViolationDuringReconnectTest {

    static class Context {
        int port;
        NatsConnection nc;
        Dispatcher d;
        CountDownLatch latch;
        ConcurrentHashMap.KeySetView<String, Boolean> subscriptions = ConcurrentHashMap.newKeySet();
        ScheduledExecutorService serverRestarter = Executors.newSingleThreadScheduledExecutor();
        ExecutorService unsubThreadpool = Executors.newFixedThreadPool(2);
        AtomicReference<NatsTestServer> ts = new AtomicReference<>();
        AtomicBoolean violated = new AtomicBoolean(false);
        AtomicInteger restartsLeft = new AtomicInteger(10);
        ErrorListener errorListener = new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {
                if (error.contains("Authorization Violation")) {
                    // System.out.println("Authorization Violation");
                    violated.set(true);
                }
            }
        };
    }

    @Test
    public void testAuthViolationDuringReconnect() throws Exception {
        Context ctx = new Context();
        ctx.port = NatsTestServer.nextPort();

        startNatsServer(ctx);

        ctx.nc = new MockPausingNatsConnection(buildOptions(ctx));
        ctx.nc.connect(true);
        ctx.d = ctx.nc.createDispatcher();
        subscribe(ctx);

        ctx.serverRestarter.scheduleWithFixedDelay(() -> restartServer(ctx), 2000, 3000, TimeUnit.MILLISECONDS);

        Thread t = new Thread(waitCloseSocket(ctx));
        t.start();
        t.join();

        assertFalse(ctx.violated.get());
        ctx.ts.get().shutdown();
    }

    private static Runnable waitCloseSocket(Context ctx) {
        return () -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (!ctx.violated.get() && ctx.restartsLeft.get() > 0) {
                if (ctx.nc.closeSocketLock.isLocked()) {
                    // System.out.printf("Unsubscribing all subscriptions due to disconnection %d \n", ctx.subscriptions.size());
                    ctx.latch.countDown();
                    // just acquire the lock and release it
                    try {
                        ctx.nc.closeSocketLock.lock();
                    }
                    finally {
                        ctx.nc.closeSocketLock.unlock();
                    }
                }
            }
        };
    }

    private static void restartServer(Context ctx) {
        try {
            ctx.restartsLeft.decrementAndGet();
            // System.out.println("Restarting server " + ctx.restartsLeft.get());
            ctx.ts.get().shutdown();
            startNatsServer(ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void startNatsServer(Context ctx) throws IOException {
        ctx.ts.set(new NatsTestServer(new String[]{"--auth", "1234"}, ctx.port, false));
    }

    private static void subscribe(Context ctx) {
        ctx.latch = new CountDownLatch(1);
        for (int i = 0; i < 1_000; i++) {
            String subject = "test_" + i;
            ctx.subscriptions.add(subject);
            ctx.d.subscribe(subject);
            ctx.unsubThreadpool.execute(() -> {
                try {
                    ctx.latch.await();
                    ctx.d.unsubscribe(subject);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static Options buildOptions(Context ctx) {
        Options.Builder natsOptions = new Options.Builder()
                .servers(new String[]{"nats://incorrect:1111", "nats://localhost:" + ctx.port})
                .noRandomize()
                .token(new char[]{'1', '2', '3', '4'})
                .reconnectWait(Duration.ofMillis(2000))
                .connectionTimeout(Duration.ofMillis(500))
                .errorListener(ctx.errorListener);

        return natsOptions.build();
    }

    private static class ReconnectedHandler implements ConnectionListener {

        private java.util.function.Consumer<Void> consumer;

        public void setConsumer(java.util.function.Consumer<Void> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void connectionEvent(Connection conn, Events type) {
            if (type == Events.RECONNECTED) {
                consumer.accept(null);
            }
        }
    }

    static class MockPausingNatsConnection extends NatsConnection {
        MockPausingNatsConnection(Options options) {
            super(options);
        }

        @Override
        void closeSocketImpl(boolean forceClose) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            super.closeSocketImpl(forceClose);
        }

        @Override
        void sendUnsub(NatsSubscription sub, int after) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            super.sendUnsub(sub, after);
        }
    }
}
