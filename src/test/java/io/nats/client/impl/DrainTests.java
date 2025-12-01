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
import io.nats.client.utils.SharedServer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.*;
import static io.nats.client.utils.TestBase.*;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class DrainTests {

    @Test
    public void testCloseOnDrainFailure() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {
            //noinspection resource
            final Connection nc = standardConnectionWait(optionsNoReconnect(ts));

            nc.subscribe(random());
            nc.flush(Duration.ofSeconds(1)); // Get the sub to the server, so drain has things to do

            ts.shutdown(); // shut down the server to fail drain and subsequent close

            assertThrows(Exception.class, () -> nc.drain(Duration.ofSeconds(1)));
        }
    }

    @Test
    public void testSimpleSubDrain() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null); // publish 2
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
        });
    }

    @Test
    public void testSimpleDispatchDrain() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(2000); // go slow so the main app can drain us
            });

            String subject = random();
            d.subscribe(subject);
            d.subscribe(subject, msg -> count.incrementAndGet());
            subCon.flush(Duration.ofSeconds(5)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));
            subCon.flush(Duration.ofSeconds(1));

            // Drain will unsub the dispatcher, only messages that already arrived
            // are there
            CompletableFuture<Boolean> tracker = d.drain(Duration.ofSeconds(8));

            assertTrue(tracker.get(10, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(4, count.get()); // Should get both, two times.
            assertFalse(d.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        });
    }

    @Test
    public void testSimpleConnectionDrain() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            String subject = random();
            d.subscribe(subject);

            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
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
        });
    }

    @Test
    public void testConnectionDrainWithZeroTimeout() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            String subject = random();
            d.subscribe(subject);

            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
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
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        });
    }

    @Test
    public void testDrainWithZeroTimeout() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null); // publish 2
            pubCon.flush(Duration.ofSeconds(1));

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);

            subCon.flush(Duration.ofSeconds(1));
            CompletableFuture<Boolean> tracker = sub.drain(Duration.ZERO);

            msg = sub.nextMessage(Duration.ofSeconds(1)); // read the second one, should be there because we drained
            assertNotNull(msg);

            assertTrue(tracker.get(1, TimeUnit.SECONDS));
            assertFalse(sub.isActive());
        });
    }

    @Test
    public void testSubDuringDrainThrows() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));
            subCon.flush(Duration.ofSeconds(1));

            subCon.drain(Duration.ofSeconds(500));

            // Try to subscribe while we are draining the sub
            assertThrows(IllegalStateException.class, () -> subCon.subscribe(random()));
        });
    }

    @Test
    public void testCreateDispatcherDuringDrainThrows() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));

            subCon.drain(Duration.ofSeconds(500));

            assertThrows(IllegalStateException.class, () -> subCon.createDispatcher(msg -> {}));
        });
    }

    @Test
    public void testUnsubDuringDrainIsNoop() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(1000); // go slow so the main app can drain us
            });
            String subject = random();
            d.subscribe(subject);

            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(5));

            sleep(1000); // give the drain time to get started

            sub.unsubscribe();
            d.unsubscribe(subject);

            Message msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(msg);

            assertTrue(tracker.get(2, TimeUnit.SECONDS));
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        });
    }

    @Test
    public void testDrainInMessageHandler() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            AtomicReference<Dispatcher> dispatcher = new AtomicReference<>();
            AtomicReference<CompletableFuture<Boolean>> tracker = new AtomicReference<>();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                tracker.set(dispatcher.get().drain(Duration.ofSeconds(1)));
            });
            String subject = random();
            d.subscribe(subject);
            dispatcher.set(d);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            assertTrue(tracker.get().get(5, TimeUnit.SECONDS)); // wait for the drain to complete
            assertEquals(2, count.get()); // Should get both
            assertFalse(d.isActive());
            assertEquals(0, ((NatsConnection) subCon).getConsumerCount());
        });
    }

    @Test
    public void testDrainFutureMatches() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            AtomicInteger count = new AtomicInteger();
            Dispatcher d = subCon.createDispatcher(msg -> {
                count.incrementAndGet();
                sleep(500); // go slow so the main app can drain us
            });
            String subject = random();
            d.subscribe(subject);

            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
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
        });
    }

    @Test
    public void testFirstTimeRequestReplyDuringDrain() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            Dispatcher d = pubCon.createDispatcher(msg -> pubCon.publish(msg.getReplyTo(), null));
            String reply = random();
            d.subscribe(reply);
            pubCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.drain(Duration.ofSeconds(500));

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);

            assertThrows(IllegalStateException.class, () -> subCon.request(reply, null));
        });
    }

    @Test
    public void testRequestReplyDuringDrain() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            String subject = random();
            Subscription sub = subCon.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            Dispatcher d = pubCon.createDispatcher(msg -> pubCon.publish(msg.getReplyTo(), null));
            String reply = random();
            d.subscribe(reply);
            pubCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            CompletableFuture<Message> response = subCon.request(reply, null);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server
            assertNotNull(response.get(1, TimeUnit.SECONDS));

            subCon.drain(Duration.ofSeconds(1));

            Message msg = sub.nextMessage(Duration.ofSeconds(1)); // read 1
            assertNotNull(msg);

            assertThrows(IllegalStateException.class, () -> subCon.request(reply, null));
        });
    }

    @Test
    public void testQueueHandoffWithDrain() throws Exception {
        runInSharedOwnNc(optionsBuilderNoReconnect(), pubCon -> {
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

            Connection draining = SharedServer.connectionForSameServer(pubCon, optionsBuilderNoReconnect());

            String subject = random();
            String queue = random();
            drainingD = (NatsDispatcher) draining.createDispatcher(msg -> count.incrementAndGet()).subscribe(subject, queue);
            draining.flush(Duration.ofSeconds(5));

            Thread pubThread = new Thread(() -> {
                for (int i = 0; i < total; i++) {
                    pubCon.publish(subject, null);
                    sleep(sleepBetweenMessages);
                }
                flushConnection(pubCon, Duration.ofSeconds(5));
            });

            pubThread.start();

            while (count.get() < total && Duration.between(start, now).compareTo(testTimeout) < 0) {
                working = SharedServer.connectionForSameServer(pubCon, optionsBuilderNoReconnect());
                assertConnected(working);
                workingD = (NatsDispatcher) working.createDispatcher(msg -> count.incrementAndGet()).subscribe(subject, queue);
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
        });
    }

    @Test
    public void testDrainWithLotsOfMessages() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
            int total = 1000;
            String subject = random();
            Subscription sub = subCon.subscribe(subject);

            sub.setPendingLimits(5 * total, -1);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            // Sub should cache them in the pending queue
            for (int i = 0; i < total; i++) {
                pubCon.publish(subject, null);
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
        });
    }

    @Test
    public void testSlowAsyncDuringDrainCanFinishIfTime() throws Exception {
        runInSharedOwnNcs(optionsBuilderNoReconnect(), (subCon, pubCon) -> {
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
            String subject = random();
            d.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(4));

            assertTrue(tracker.get(10, TimeUnit.SECONDS));
            assertTrue(((NatsConnection) subCon).isDrained());
            assertEquals(2, count.get()); // Should get both
            assertClosed(subCon);
        });
    }

    @Test
    public void testSlowAsyncDuringDrainCanBeInterrupted() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        runInSharedOwnNc(optionsBuilder().errorListener(listener).maxReconnects(0), subCon -> {
            Connection pubCon = SharedServer.sharedConnectionForSameServer(subCon);
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
            String subject = random();
            d.subscribe(subject);
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            pubCon.publish(subject, null);
            pubCon.publish(subject, null);
            pubCon.flush(Duration.ofSeconds(1));

            subCon.flush(Duration.ofSeconds(1));
            sleep(500); // give the msgs time to get to subCon

            assertEquals(0, listener.getExceptionCount());
            CompletableFuture<Boolean> tracker = subCon.drain(Duration.ofSeconds(2));

            assertFalse(tracker.get(10, TimeUnit.SECONDS));
            assertFalse(((NatsConnection) subCon).isDrained());
            assertEquals(0, listener.getExceptionCount()); // Don't throw during drain from reader
            assertClosed(subCon);
        });
    }

    @Test
    public void testThrowIfCantFlush() throws Exception {
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer();
             Connection subCon = standardConnectionWait(optionsBuilder(ts).connectionListener(listener).build())) {
            subCon.flush(Duration.ofSeconds(1)); // Get the sub to the server

            ListenerFuture f = listener.prepForConnectionEvent(Events.DISCONNECTED);
            ts.close(); // make the drain flush fail
            listener.validateReceived(f);

            assertThrows(TimeoutException.class, () -> subCon.drain(Duration.ofSeconds(1)));
        }
    }

    @Test
    public void testThrowIfClosing() throws Exception {
        runInSharedOwnNc(optionsBuilderNoReconnect(), subCon -> {
            subCon.close();
            assertThrows(IllegalStateException.class, () -> subCon.drain(Duration.ofSeconds(1)));
        });
    }
}
