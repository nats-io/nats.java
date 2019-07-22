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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.nats.client.TestHandler;
import io.nats.client.ConnectionListener.Events;

public class DrainTests {

    @Test
    public void testSimpleSubDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

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
            assertEquals(((NatsConnection) subCon).getConsumerCount(), 0);
        }
    }

    @Test
    public void testSimpleDispatchDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
                count.incrementAndGet();
                try {
                    Thread.sleep(2000); // go slow so the main app can drain us
                } catch (Exception e) {

                }
            });
            d.subscribe("draintest");
            d.subscribe("draintest", (msg) -> { count.incrementAndGet(); });
            subCon.flush(Duration.ofSeconds(5)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));
            subCon.flush(Duration.ofSeconds(1));

            // Drain will unsub the dispatcher, only messages that already arrived
            // are there
            CompletableFuture<Boolean> tracker = d.drain(Duration.ofSeconds(8));

            assertTrue(tracker.get(10, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(count.get(), 4); // Should get both, two times.
            assertFalse(d.isActive());
            assertEquals(((NatsConnection) subCon).getConsumerCount(), 0);
        }
    }

    @Test
    public void testSimpleConnectionDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
                count.incrementAndGet();
                try {
                    Thread.sleep(500); // go slow so the main app can drain us
                } catch (Exception e) {

                }
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(count.get(), 2); // Should get both
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test
    public void testConnectionDrainWithZeroTimeout() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
                count.incrementAndGet();
                try {
                    Thread.sleep(500); // go slow so the main app can drain us
                } catch (Exception e) {

                }
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            CompletableFuture<Boolean> tracker = subCon.drain(null);

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(count.get(), 2); // Should get both
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test
    public void testDrainWithZeroTimeout() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

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

    @Test(expected = IllegalStateException.class)
    public void testSubDuringDrainThrows() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

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
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateDispatcherDuringDrainThrows() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(500));

            subCon.createDispatcher((msg) -> {
            });
            assertTrue(tracker.get(1000, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testUnsubDuringDrainIsNoop() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
                count.incrementAndGet();
                try {
                    Thread.sleep(1000); // go slow so the main app can drain us
                } catch (Exception e) {

                }
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            try {
                Thread.sleep(1000); // give the drain time to get started
            } catch (Exception e) {

            }

            sub.unsubscribe();
            d.unsubscribe("draintest");

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertEquals(count.get(), 2); // Should get both
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test
    public void testDrainInMessageHandler() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            AtomicReference<Dispatcher> dispatcher = new AtomicReference<>();
            AtomicReference<CompletableFuture<Boolean>> tracker = new AtomicReference<>();
            Dispatcher d = subCon.createDispatcher((msg) -> {
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
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            assertTrue(tracker.get().get(5, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(count.get(), 2); // Should get both
            assertFalse(d.isActive());
            assertEquals(((NatsConnection) subCon).getConsumerCount(), 0);
        }
    }

    @Test
    public void testDrainFutureMatches() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
                count.incrementAndGet();
                try {
                    Thread.sleep(500); // go slow so the main app can drain us
                } catch (Exception e) {

                }
            });
            d.subscribe("draintest");

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish("draintest", null);
            pubCon.publish("draintest", null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            assertTrue(tracker == sub.drain(Duration.ZERO));
            assertTrue(tracker == sub.drain(Duration.ZERO));
            assertTrue(tracker == d.drain(Duration.ZERO));
            assertTrue(tracker == d.drain(Duration.ZERO));
            assertTrue(tracker == subCon.drain(Duration.ZERO));
            assertTrue(tracker == subCon.drain(Duration.ZERO));

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertEquals(count.get(), 2); // Should get both
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testFirstTimeRequestReplyDuringDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            Dispatcher d = pubCon.createDispatcher((msg) -> {
                pubCon.publish(msg.getReplyTo(), null);
            });
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
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testRequestReplyDuringDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            Subscription sub = subCon.subscribe("draintest");
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            Dispatcher d = pubCon.createDispatcher((msg) -> {
                pubCon.publish(msg.getReplyTo(), null);
            });
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
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test
    public void testQueueHandoffWithDrain() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            final int total = 5_000;
            final Duration sleepBetweenDrains = Duration.ofMillis(250);
            final Duration sleepBetweenMessages = Duration.ofMillis(1);
            final Duration testTimeout = Duration.ofMillis(5 * total * sleepBetweenMessages.toMillis());
            final Duration drainTimeout = testTimeout;
            final Duration waitTimeout = drainTimeout.plusSeconds(1);
            AtomicInteger count = new AtomicInteger();
            Instant start = Instant.now();
            Instant now = start;
            Connection working = null;
            NatsDispatcher workingD = null;
            NatsDispatcher drainingD = null;

            Connection draining = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
            assertTrue("Connected Status", Connection.Status.CONNECTED == draining.getStatus());

            drainingD = (NatsDispatcher) draining.createDispatcher((msg) -> {
                count.incrementAndGet();
            }).subscribe("draintest", "queue");
            draining.flush(Duration.ofSeconds(5));

            Thread pubThread = new Thread(() -> {
                for (int i = 0; i < total; i++) {
                    pubCon.publish("draintest", null);
                    try {
                        LockSupport.parkNanos(sleepBetweenMessages.toNanos()); // use a nice stead pace to avoid slow consumers
                    } catch (Exception e) {

                    }
                }
                try {
                    pubCon.flush(Duration.ofSeconds(5));
                } catch (Exception e) {

                }
            });

            pubThread.start();

            while (count.get() < total && Duration.between(start, now).compareTo(testTimeout) < 0) {

                working = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                assertTrue("Connected Status", Connection.Status.CONNECTED == working.getStatus());
                workingD = (NatsDispatcher) working.createDispatcher((msg) -> {
                    count.incrementAndGet();
                }).subscribe("draintest", "queue");
                working.flush(Duration.ofSeconds(5));

                try {
                    LockSupport.parkNanos(sleepBetweenDrains.toNanos()); // let them both work a bit
                } catch (Exception e) {

                }

                CompletableFuture<Boolean> tracker = draining.drain(drainTimeout);

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

            assertEquals(count.get(), total);
        }
    }

    @Test
    public void testDrainWithLotsOfMessages() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            int total = 1000;
            Subscription sub = subCon.subscribe("draintest");

            sub.setPendingLimits(5 * total, -1);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            // Sub should cache them in the pending queue
            for (int i = 0; i < total; i++) {
                pubCon.publish("draintest", null);
                try {
                    Thread.sleep(1); // use a nice stead pace to avoid slow consumers
                } catch (Exception e) {

                }
            }
            try {
                pubCon.flush(Duration.ofSeconds(5));
            } catch (Exception e) {

            }

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
            assertEquals(((NatsConnection) subCon).getConsumerCount(), 0);
        }
    }

    @Test
    public void testSlowAsyncDuringDrainCanFinishIfTime() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
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
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(4));

            assertTrue(tracker.get(5, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(count.get(), 2); // Should get both
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test
    public void testSlowAsyncDuringDrainCanBeInterrupted() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).errorListener(handler).maxReconnects(0).build());
                Connection pubCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            assertTrue("Connected Status", Connection.Status.CONNECTED == pubCon.getStatus());

            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher((msg) -> {
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
            try {
                Thread.sleep(500); // give the msgs time to get to subCon
            } catch (Exception e) {

            }

            assertTrue(handler.getExceptionCount() == 0);
            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(2));

            assertFalse(tracker.get(3, TimeUnit.SECONDS));
            assertFalse(((NatsConnection) subCon).isDrained());
            assertTrue(handler.getExceptionCount() == 0); // Don't throw during drain from reader
            assertTrue(Connection.Status.CLOSED == subCon.getStatus());
        }
    }

    @Test(expected=TimeoutException.class)
    public void testThrowIfCantFlush() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().connectionListener(handler).server(ts.getURI()).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            handler.prepForStatusChange(Events.DISCONNECTED);
            ts.close(); // make the drain flush fail
            handler.waitForStatusChange(2, TimeUnit.SECONDS); // make sure the connection is down
            subCon.drain(Duration.ofSeconds(1)); //should throw
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowIfClosing() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection subCon = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == subCon.getStatus());

            subCon.close();
            subCon.drain(Duration.ofSeconds(1));
        }
    }
}
