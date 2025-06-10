package io.nats.client.impl;

import io.nats.client.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/* Program to reproduce #1320 */
public class AuthViolationDuringReconnect {
    private static final ConcurrentHashMap.KeySetView<String, Boolean> subscriptions = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService serverRestarter = Executors.newSingleThreadScheduledExecutor();
    private static final ExecutorService unsubThreadpool = Executors.newFixedThreadPool(64);
    private static final AtomicReference<NatsTestServer> ts = new AtomicReference<>();
    private static final ErrorListener AUTHORIZATION_VIOLATION_LISTENER = new ErrorListener() {
        @Override
        public void errorOccurred(Connection conn, String error) {
            if (error.contains("Authorization Violation")) {
                System.out.println("Authorization Violation, Stopping server");
                try {
                    Thread.sleep(1000);
                    ts.get().shutdown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.exit(-1);
            }
        }
    };
    private static volatile CountDownLatch latch;

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = NatsTestServer.nextPort();
        ts.set(new NatsTestServer(new String[]{"--auth", "1234", "-m", "8222"}, port, false));

        ReconnectedHandler reconnectedHandler = new ReconnectedHandler();
        NatsConnection nc = (NatsConnection) Nats.connect(buildOptions(port, reconnectedHandler));
        Dispatcher d = nc.createDispatcher();

        reconnectedHandler.setConsumer((ignored) -> subscribe(d));
        subscribe(d);

        serverRestarter.scheduleWithFixedDelay(() -> restartServer(ts, port), 2, 1, TimeUnit.SECONDS);

        new Thread(waitCloseSocket(nc)).start();
    }

    private static Runnable waitCloseSocket(NatsConnection nc) {
        return () -> {
            while (true) {
                if (nc.closeSocketLock.isLocked()) {
                    try {
                        System.out.printf("Unsubscribing all subscriptions due to disconnection %d \n", subscriptions.size());
                        latch.countDown();
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                }
                LockSupport.parkNanos(5);
            }
        };
    }

    private static void restartServer(AtomicReference<NatsTestServer> ts, int port) {
        try {
            ts.get().shutdown();
            ts.set(new NatsTestServer(new String[]{"--auth", "1234", "-m", "8222"}, port, false));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void subscribe(Dispatcher d) {
        latch = new CountDownLatch(1);
        for (int i = 0; i < 300_000; i++) {
            String subject = "test_" + i;
            subscriptions.add(subject);
            d.subscribe(subject);
            unsubThreadpool.execute(() -> {
                try {
                    latch.await();
                    d.unsubscribe(subject);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static Options buildOptions(int port, ReconnectedHandler reconnectedHandler) {
        Options.Builder natsOptions = new Options.Builder()
                .servers(new String[]{"nats://localhost:" + port})
                .token(new char[]{'1', '2', '3', '4'})
                .maxReconnects(-1)
                .reconnectWait(Duration.ofMillis(200))
                .connectionTimeout(Duration.ofMillis(500))
                .connectionListener(reconnectedHandler)
                .errorListener(AUTHORIZATION_VIOLATION_LISTENER);

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
}
