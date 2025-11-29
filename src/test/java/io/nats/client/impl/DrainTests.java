// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.TestBase.flushConnection;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class DrainTests {

    @SuppressWarnings("resource")
    @Test
    public void testCloseOnDrainFailure() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            final Connection nc = standardConnectionWait(optionsBuilder(ts).maxReconnects(0).build());

            nc.subscribe("draintest");
            nc.flush(Duration.ofSeconds(1)); // Get the sub to the server, so drain has things to do

            ts.shutdown(); // shut down the server to fail drain and subsequent close

            assertThrows(Exception.class, () -> nc.drain(Duration.ofSeconds(1)));
        }
    }

    @Test
    public void testSimpleSubDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null); // publish 2
            pubCon.flush(Duration.ofSeconds(1));

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);

            subCon.flush(Duration.ofSeconds(1));
            CompletableFuture<Boolean> tracker = sub.drain(Duration.ofSeconds(1));

            msg = sub.nextMessage(Duration.ofSeconds(1)); // read the second one, should be there because we drained
            assertNotNull(msg);

            assertTrue(tracker.get(1, TimeUnit.SECONDS));
            assertFalse(sub.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        }
    }

    @Test
    public void testSimpleDispatchDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(2000); // go slow so the main app can drain us
            });
            d.subscribe("draintest");
            d.subscribe("draintest", msg -> count.incrementAndGet());
            subCon.flush(Duration.ofSeconds(5)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));
            subCon.flush(Duration.ofSeconds(1));

            // Drain will unsub the dispatcher, only messages that already arrived
            // are there
            CompletableFuture<Boolean> tracker = d.drain(Duration.ofSeconds(8));

            assertTrue(tracker.get(10, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(4, count.get()); // Should get both, two times.
            assertFalse(d.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        }
    }

    @Test
    public void testSimpleConnectionDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        }
    }

    @Test
    public void testConnectionDrainWithZeroTimeout() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(null);

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(count.get(), 2); // Should get both
            assertClosed(subCon);
        }
    }

    @Test
    public void testDrainWithZeroTimeout() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null); // publish 2
            pubCon.flush(Duration.ofSeconds(1));

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);

            subCon.flush(Duration.ofSeconds(1));
            CompletableFuture<Boolean> tracker = sub.drain(Duration.ZERO);

            msg = sub.nextMessage(Duration.ofSeconds(1)); // read the second one, should be there because we drained
            assertNotNull(msg);

            assertTrue(tracker.get(1, TimeUnit.SECONDS));
            assertFalse(sub.isActive());
        }
    }

    @Test
    public void testSubDuringDrainThrows() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer();
                 Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
                 Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
                assertConnected(subCon);
                assertConnected(pubCon);

                subCon.subscribe("draintest");
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                pubCon.publish("draintest", null);
                pubCon.publish("draintest", null);
                pubCon.flush(Duration.ofSeconds(1));

                subCon.flush(Duration.ofSeconds(1));

                CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(500));

                // Try to subscribe while we are draining the sub
                subCon.subscribe("another"); // Should throw
                assertTrue(tracker.get(1000, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testCreateDispatcherDuringDrainThrows() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer();
                 Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
                 Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
                assertConnected(subCon);
                assertConnected(pubCon);

                subCon.subscribe("draintest");
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                pubCon.publish("draintest", null);
                pubCon.publish("draintest", null);
                pubCon.flush(Duration.ofSeconds(1));

                subCon.flush(Duration.ofSeconds(1));

                CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(500));

                subCon.createDispatcher(msg -> {
                });
                assertTrue(tracker.get(1000, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testUnsubDuringDrainIsNoop() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(1000); // go slow so the main app can drain us
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            sleep(1000); // give the drain time to get started

            sub.unsubscribe();
            d.unsubscribe("draintest");

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        }
    }

    @Test
    public void testDrainInMessageHandler() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            AtomicReference<Dispatcher> dispatcher = new AtomicReference<>();
            AtomicReference<CompletableFuture<Boolean>> tracker = new AtomicReference<>();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                tracker.set(dispatcher.get().drain(Duration.ofSeconds(1)));
            });
            d.subscribe("draintest");
            dispatcher.set(d);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            assertTrue(tracker.get().get(5, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(2, count.get()); // Should get both
            assertFalse(d.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        }
    }

    @Test
    public void testDrainFutureMatches() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            assertSame(tracker, sub.drain(Duration.ZERO));
            assertSame(tracker, sub.drain(Duration.ZERO));
            assertSame(tracker, d.drain(Duration.ZERO));
            assertSame(tracker, d.drain(Duration.ZERO));
            assertSame(tracker, subCon.drain(Duration.ZERO));
            assertSame(tracker, subCon.drain(Duration.ZERO));

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        }
    }

    @Test
    public void testFirstTimeRequestReplyDuringDrain() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer();
                 Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
                 Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
                assertConnected(subCon);
                assertConnected(pubCon);

                Subscription sub = subCon.subscribe("draintest");
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                Dispatcher d = pubCon.createDispatcher(msg -> pubCon.publish(msg.getReplyTo(), null));
                d.subscribe("reply");
                pubCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                pubCon.publish("draintest", null);
                pubCon.publish("draintest", null);
                pubCon.flush(Duration.ofSeconds(1));

                CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(500));

                Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
                assertNotNull(msg);

                CompletableFuture<Message> response = subCon.request("reply", null);
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server
                assertNotNull(response.get(200, TimeUnit.SECONDS));

                msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
                assertNotNull(msg);

                assertTrue(tracker.get(500, TimeUnit.SECONDS)); // wait for the drain to complete
                assertClosed(subCon);
            }
        });
    }

    @Test
    public void testRequestReplyDuringDrain() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer();
                 Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
                 Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
                assertConnected(subCon);
                assertConnected(pubCon);

                Subscription sub = subCon.subscribe("draintest");
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                Dispatcher d = pubCon.createDispatcher(msg -> pubCon.publish(msg.getReplyTo(), null));
                d.subscribe("reply");
                pubCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                pubCon.publish("draintest", null);
                pubCon.publish("draintest", null);
                pubCon.flush(Duration.ofSeconds(1));

                CompletableFuture<Message> response = subCon.request("reply", null);
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server
                assertNotNull(response.get(1, TimeUnit.SECONDS));

                CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(1));

                Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
                assertNotNull(msg);

                response = subCon.request("reply", null);
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server
                assertNotNull(response.get(200, TimeUnit.SECONDS));

                msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
                assertNotNull(msg);

                assertTrue(tracker.get(500, TimeUnit.SECONDS)); // wait for the drain to complete
                assertClosed(subCon);
            }
        });
    }

    @Test
    public void testQueueHandoffWithDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
                Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(pubCon);

            final int total = 5_000;
            final long sleepBetweenDrains = 250;
            final long sleepBetweenMessages = 5;
            final Duration testTimeout = Duration.ofMillis(5 * total * (sleepBetweenDrains + sleepBetweenMessages));
            final Duration waitTimeout = testTimeout.plusSeconds(1);
            AtomicInteger count = new AtomicInteger();
            Instant start = Instant.now();
            Instant now = start;
            Connection working;
            NatsDispatcher workingD;
            NatsDispatcher drainingD;

            Connection draining = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
            assertConnected(draining);

            drainingD = (NatsDispatcher) draining.createDispatcher(msg -> count.incrementAndGet()).subscribe("draintest", "queue");
            draining.flush(Duration.ofSeconds(5));

            Thread pubThread = new Thread(() -> {
                for (int i = 0; i < total; i++) {
                    pubCon.publish("draintest", null);
                    sleep(sleepBetweenMessages);
                }
                flushConnection(pubCon, Duration.ofSeconds(5));
            });

            pubThread.start();

            while (count.get() < total && Duration.between(start, now).compareTo(testTimeout) < 0) {

                working = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
                assertConnected(working);
                workingD = (NatsDispatcher) working.createDispatcher(msg -> count.incrementAndGet()).subscribe("draintest", "queue");
                working.flush(Duration.ofSeconds(5));

                sleep(sleepBetweenDrains);

                CompletableFuture<Boolean> tracker = draining.drain(testTimeout);

                assertTrue(tracker.get(waitTimeout.toMillis(), TimeUnit.MILLISECONDS)); // wait for the drain to complete
                assertTrue(drainingD.isDrained());
                assertTrue(((NatsConnection) draining).isDrained());
                draining.close(); // no op, but ide wants this for auto-closable

                draining = working;
                drainingD = workingD;
                now = Instant.now();
            }

            draining.close();
            pubThread.join();

            assertEquals(total, count.get());
        }
    }

    @Test
    public void testDrainWithLotsOfMessages() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            int total = 1000;
            Subscription sub = subCon.subscribe("draintest");

            sub.setPendingLimits(5 * total, -1);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            // Sub should cache them in the pending queue
            for (int i = 0; i < total; i++) {
                pubCon.publish("draintest", null);
                sleep(1); // use a nice stead pace to avoid slow consumers
            }
            try {
                pubCon.flush(Duration.ofSeconds(5));
            } catch (Exception ignored) {}

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);
            subCon.flush(Duration.ofSeconds(1));

            CompletableFuture<Boolean> tracker = sub.drain(Duration.ofSeconds(10));

            for (int i = 1; i < total; i++) { // we read 1 so start there
                msg = sub.nextMessage(Duration.ofSeconds(1)); // read the second one, should be there because we drained
                assertNotNull(msg);
            }

            assertTrue(tracker.get(5, TimeUnit.SECONDS));
            assertFalse(sub.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        }
    }

    @Test
    public void testSlowAsyncDuringDrainCanFinishIfTime() throws Exception {
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                try {
                    Thread.sleep(1500); // go slow so the main app can drain us
                } catch (Exception e) {
                    assertNull(e);
                }

                if (!Thread.interrupted()) {
                    count.incrementAndGet();
                }
            });
            d.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(4));

            assertTrue(tracker.get(10, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        }
    }

    @Test
    public void testSlowAsyncDuringDrainCanBeInterrupted() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = Nats.connect(optionsBuilder(ts).errorListener(listener).maxReconnects(0).build());
             Connection pubCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
            assertConnected(subCon);
            assertConnected(pubCon);

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                try {
                    Thread.sleep(3000); // go slow so the main app can drain us
                } catch (Exception e) {
                    assertNull(e);
                }

                if (!Thread.interrupted()) {
                    count.incrementAndGet();
                }
            });
            d.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            assertEquals(0, listener.getExceptionCount());
            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(2));

            assertFalse(tracker.get(10, TimeUnit.SECONDS));
            assertFalse(((NatsConnection) subCon).isDrained());
            assertEquals(0, listener.getExceptionCount()); // Don't throw during drain from reader
            assertClosed(subCon);
        }
    }

    @Test
    public void testThrowIfCantFlush() {
        assertThrows(TimeoutException.class, () -> {
            ListenerForTesting listener = new ListenerForTesting();
            try (NatsTestServer ts = new NatsTestServer();
                 Connection subCon = standardConnectionWait(optionsBuilder(ts).connectionListener(listener).build()))
            {
                subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

                listener.prepForStatusChange(Events.DISCONNECTED);
                ts.close(); // make the drain flush fail
                listener.waitForStatusChange(2, TimeUnit.SECONDS); // make sure the connection is down
                subCon.drain(Duration.ofSeconds(1)); //should throw
            }
        });
    }

    @Test
    public void testThrowIfClosing() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer();
                    Connection subCon = Nats.connect(optionsBuilder(ts).maxReconnects(0).build())) {
                assertConnected(subCon);

                subCon.close();
                subCon.drain(Duration.ofSeconds(1));
            }
        });
    }
}
