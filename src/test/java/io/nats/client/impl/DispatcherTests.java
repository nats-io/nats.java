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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.TestBase.sleep;
import static io.nats.client.utils.TestBase.waitUntilStatus;
import static org.junit.jupiter.api.Assertions.*;



// Some tests are a bit tricky, and depend on the fact that the dispatcher
// uses a single queue, so the "subject" messages go through before
// the done message (or should) - wanted to note that somewhere

public class DispatcherTests {
    @Test
    public void testSingleMessage() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                msgFuture.complete(msg);
            });

            d.subscribe("subject");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

            assertTrue(d.isActive());
            assertEquals("subject", msg.getSubject());
            assertNotNull(msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
        }
    }

    @Test
    public void testDispatcherMessageContainsConnection() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            final CompletableFuture<Connection> connFuture = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                msgFuture.complete(msg);
                connFuture.complete(msg.getConnection());
            });

            d.subscribe("subject");
            nc.flush(Duration.ofMillis(5000));// Get them all to the server

            nc.publish("subject", new byte[16]);

            Message msg = msgFuture.get(5000, TimeUnit.MILLISECONDS);
            Connection conn = connFuture.get(5000, TimeUnit.MILLISECONDS);

            assertTrue(d.isActive());
            assertEquals("subject", msg.getSubject());
            assertNotNull(msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
            assertTrue(conn == nc);
        }
    }

    @Test
    public void testMultiSubject() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final CompletableFuture<Message> one = new CompletableFuture<>();
            final CompletableFuture<Message> two = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("one")) {
                    one.complete(msg);
                } else if (msg.getSubject().equals("two")) {
                    two.complete(msg);
                }
            });

            d.subscribe("one");
            d.subscribe("two");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("one", new byte[16]);
            nc.publish("two", new byte[16]);

            Message msg = one.get(500, TimeUnit.MILLISECONDS);
            assertEquals("one", msg.getSubject());
            msg = two.get(500, TimeUnit.MILLISECONDS);
            assertEquals("two", msg.getSubject());
        }
    }

    @Test
    public void testMultiMessage() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                }
            });

            d.subscribe("subject");
            d.subscribe("done");
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            done.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgCount, q.size());
        }
    }

    @Test
    public void testClose() {
        assertThrows(TimeoutException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
                final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
                final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
                Dispatcher d = nc.createDispatcher((msg) -> {
                    if (msg.getSubject().equals("phase1")) {
                        phase1.complete(Boolean.TRUE);
                    } else if (msg.getSubject().equals("phase1")) {
                        phase2.complete(Boolean.TRUE);
                    } else {
                        q.add(msg);
                    }
                });

                d.subscribe("subject");
                d.subscribe("phase1");
                d.subscribe("phase2");
                nc.flush(Duration.ofMillis(500));// Get them all to the server

                nc.publish("subject", new byte[16]);
                nc.publish("phase1", null);

                nc.flush(Duration.ofMillis(1000)); // wait for them to go through
                phase1.get(200, TimeUnit.MILLISECONDS);

                assertEquals(1, q.size());

                nc.closeDispatcher(d);

                assertFalse(d.isActive());

                // This won't arrive
                nc.publish("phase2", new byte[16]);

                nc.flush(Duration.ofMillis(1000)); // wait for them to go through
                phase2.get(200, TimeUnit.MILLISECONDS);
            }
        });
    }


    @Test
    public void testQueueSubscribers() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI())) {
            int msgs = 100;
            AtomicInteger received = new AtomicInteger();
            AtomicInteger sub1Count = new AtomicInteger();
            AtomicInteger sub2Count = new AtomicInteger();

            final CompletableFuture<Boolean> done1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> done2 = new CompletableFuture<>();

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            Dispatcher d1 = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done1.complete(Boolean.TRUE);
                } else {
                    sub1Count.incrementAndGet();
                    received.incrementAndGet();
                }
            });

            Dispatcher d2 = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done2.complete(Boolean.TRUE);
                } else {
                    sub2Count.incrementAndGet();
                    received.incrementAndGet();
                }
            });

            d1.subscribe("subject", "queue");
            d2.subscribe("subject", "queue");
            d1.subscribe("done");
            d2.subscribe("done");
            nc.flush(Duration.ofMillis(500));

            for (int i = 0; i < msgs; i++) {
                nc.publish("subject", new byte[16]);
            }

            nc.publish("done", null);

            nc.flush(Duration.ofMillis(500));
            done1.get(500, TimeUnit.MILLISECONDS);
            done2.get(500, TimeUnit.MILLISECONDS);

            assertEquals(msgs, received.get());
            assertEquals(msgs, sub1Count.get() + sub2Count.get());

            // They won't be equal but print to make sure they are close (human testing)
            System.out.println("### Sub 1 " + sub1Count.get());
            System.out.println("### Sub 2 " + sub2Count.get());
        }
    }

    @Test
    public void testCantUnsubSubFromDispatcher() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI()))
            {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
                Dispatcher d = nc.createDispatcher((msg) -> {
                    msgFuture.complete(msg);
                });

                d.subscribe("subject");
                nc.flush(Duration.ofMillis(500));// Get them all to the server

                nc.publish("subject", new byte[16]);

                Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

                msg.getSubscription().unsubscribe(); // Should throw
                assertFalse(true);
            }
        });
    }

    @Test
    public void testCantAutoUnsubSubFromDispatcher() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI()))
            {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
                Dispatcher d = nc.createDispatcher((msg) -> {
                    msgFuture.complete(msg);
                });

                d.subscribe("subject");
                nc.flush(Duration.ofMillis(500));// Get them all to the server

                nc.publish("subject", new byte[16]);

                Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);

                msg.getSubscription().unsubscribe(1); // Should throw
                assertFalse(true);
            }
        });
    }

    @Test
    public void testPublishAndFlushFromCallback()
            throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                try {
                    nc.flush(Duration.ofMillis(1000));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                msgFuture.complete(msg);
            });

            d.subscribe("subject");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]); // publish one to kick it off

            Message msg = msgFuture.get(500, TimeUnit.MILLISECONDS);
            assertNotNull(msg);

            assertEquals(2, ((NatsStatistics)(nc.getStatistics())).getFlushCounter());
        }
    }

    @Test
    public void testUnsub() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 10;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("phase1")) {
                    phase1.complete(Boolean.TRUE);
                } else if (msg.getSubject().equals("phase2")) {
                    phase2.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                }
            });

            d.subscribe("subject");
            d.subscribe("phase1");
            d.subscribe("phase2");
            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase1", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            phase1.get(5000, TimeUnit.MILLISECONDS);

            d.unsubscribe("subject");
            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase2", new byte[16]);
            nc.flush(Duration.ofMillis(1000)); // wait for them to go through

            phase2.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount, q.size());
        }
    }

    @Test
    public void testAutoUnsub() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            NatsDispatcher d = (NatsDispatcher) nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("phase1")) {
                    phase1.complete(Boolean.TRUE);
                }else if (msg.getSubject().equals("phase2")) {
                    phase2.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                }
            });

            d.subscribe("subject");
            d.subscribe("phase1");
            d.subscribe("phase2");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase1", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            phase1.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount, q.size());

            d.unsubscribe("subject", msgCount + 1);

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("phase2", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // Wait for it all to get processed
            phase2.get(1000, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(msgCount + 1, q.size());
        }
    }

    @Test
    public void testUnsubFromCallback() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final AtomicReference<Dispatcher> dispatcher = new AtomicReference<>();
            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            final Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                    dispatcher.get().unsubscribe("subject");
                }
            });

            dispatcher.set(d);

            d.subscribe("subject");
            d.subscribe("done");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(1000)); // Wait for the publish, or we will get multiples for sure
            done.get(200, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(1, q.size());
        }
    }

    @Test
    public void testAutoUnsubFromCallback()
            throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final AtomicReference<Dispatcher> dispatcher = new AtomicReference<>();
            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            final Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                    dispatcher.get().unsubscribe("subject", 2); // get 1 more, for a total of 2
                }
            });

            dispatcher.set(d);

            d.subscribe("subject");
            d.subscribe("done");
            nc.flush(Duration.ofMillis(1000));// Get them all to the server

            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(1000)); // Wait for the publish

            done.get(200, TimeUnit.MILLISECONDS); // make sure we got them

            assertEquals(2, q.size());
        }
    }

    @Test
    public void testCloseFromCallback() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            final Dispatcher d = nc.createDispatcher((msg) -> {
                try {
                    if (msg.getSubject().equals("done")) {
                        nc.close();
                        done.complete(Boolean.TRUE);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            d.subscribe("done");
            sleep(500); // Making sure the "subscribe" has been registered on the server

            nc.publish("done", new byte[16]);

            done.get(5000, TimeUnit.MILLISECONDS);

            waitUntilStatus(nc, 5000, Connection.Status.CLOSED);
        }
    }

    @Test
    public void testDispatchHandlesExceptionInHandler() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                    throw new NumberFormatException();
                }
            });

            d.subscribe("subject");
            d.subscribe("done");
            nc.flush(Duration.ofMillis(500));// Get them all to the server

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);

            nc.flush(Duration.ofMillis(1000)); // wait for them to go through
            done.get(200, TimeUnit.MILLISECONDS);

            assertEquals(msgCount, q.size());
        }
    }

    @Test
    public void testThrowOnNullSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe(null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptySubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});

                d.subscribe("");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptyQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("subject", "");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnNullSubjectWithQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe(null, "quque");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptySubjectWithQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("", "quque");
                assertFalse(true);
            }
        });
    }

    @Test
    public void throwsOnCreateIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.createDispatcher((msg) -> {});
                assertFalse(true);
            }
        });
    }

    @Test
    public void throwsOnSubscribeIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                nc.close();
                d.subscribe("subject");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnSubscribeWhenClosed() throws IOException, InterruptedException, TimeoutException {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                nc.closeDispatcher(d);
                d.subscribe("foo");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnUnsubscribeWhenClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("foo");
                nc.closeDispatcher(d);
                d.unsubscribe("foo");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnDoubleClose() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                nc.closeDispatcher(d);
                nc.closeDispatcher(d);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnConnClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                nc.close();
                nc.closeDispatcher(d);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testDoubleSubscribe() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final ConcurrentLinkedQueue<Message> q = new ConcurrentLinkedQueue<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                if (msg.getSubject().equals("done")) {
                    done.complete(Boolean.TRUE);
                } else {
                    q.add(msg);
                }
            });

            d.subscribe("subject").subscribe("subject").subscribe("subject").subscribe("done");
            nc.flush(Duration.ofSeconds(5)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofSeconds(5)); // wait for them to go through

            done.get(5, TimeUnit.SECONDS);

            assertEquals(msgCount, q.size()); // Shoudl only get one since all the extra subs do nothing??
        }
    }

    @Test
    public void testDoubleSubscribeWithCustomHandler() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final AtomicInteger count = new AtomicInteger(0);
            Dispatcher d = nc.createDispatcher((msg) -> {});

            d.subscribe("subject", (msg) -> { count.incrementAndGet(); });
            d.subscribe("subject", "queue", (msg) -> { count.incrementAndGet(); });
            d.subscribe("done", (msg) -> { done.complete(Boolean.TRUE); });

            nc.flush(Duration.ofSeconds(5)); // wait for them to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofSeconds(5)); // wait for them to go through

            done.get(5, TimeUnit.SECONDS);

            assertEquals(msgCount * 2, count.get()); // We should get 2x the messages because we subscribed 2 times.
        }
    }

    @Test
    public void testDoubleSubscribeWithUnsubscribeAfterWithCustomHandler() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> done2 = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            final AtomicInteger count = new AtomicInteger(0);
            Dispatcher d = nc.createDispatcher((msg) -> {});
            Subscription s1 = d.subscribe("subject", (msg) -> { count.incrementAndGet(); });
            Subscription doneSub = d.subscribe("done", (msg) -> { done1.complete(Boolean.TRUE); });
            d.subscribe("subject", (msg) -> { count.incrementAndGet(); });

            nc.flush(Duration.ofSeconds(5)); // wait for the subs to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofSeconds(5)); // wait for the messages to go through

            done1.get(5, TimeUnit.SECONDS);

            assertEquals(msgCount * 2, count.get()); // We should get 2x the messages because we subscribed 2 times.

            count.set(0);
            d.unsubscribe(s1);
            d.unsubscribe(doneSub);
            d.subscribe("done", (msg) -> { done2.complete(Boolean.TRUE); });
            nc.flush(Duration.ofSeconds(5)); // wait for the unsub to go through

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }
            nc.publish("done", new byte[16]);
            nc.flush(Duration.ofSeconds(5)); // wait for the messages to go through

            done2.get(5, TimeUnit.SECONDS);

            assertEquals(msgCount, count.get()); // We only have 1 active subscription, so we should only get msgCount.
        }
    }

    @Test
    public void testThrowOnEmptySubjectWithMessageHandler() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("", (msg) -> {});
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnNullHandler() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("test", (MessageHandler)null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnNullHandlerWithQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("test", "queue", (MessageHandler)null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptyQueueWithMessageHandler() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("subject", "", (msg) -> {});
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnNullSubjectWithQueueWithMessageHandler() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe(null, "quque", (msg) -> {});
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptySubjectWithQueueWithMessageHandler() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.subscribe("", "quque", (msg) -> {});
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnEmptySubjectInUnsub() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                d.unsubscribe("");
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnUnsubWhenClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                Subscription sub = d.subscribe("subject", (msg) -> {});
                nc.closeDispatcher(d);
                d.unsubscribe(sub);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowOnWrongSubscription() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI())) {
                Dispatcher d = nc.createDispatcher((msg) -> {});
                Subscription sub2 = nc.subscribe("test");
                d.unsubscribe(sub2);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testDispatcherFactoryCoverage() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false);
             Connection nc = Nats.connect(Options.builder().server(ts.getURI()).useDispatcherWithExecutor().build()))
        {
            CountDownLatch latch = new CountDownLatch(1);
            Dispatcher d = nc.createDispatcher((msg) -> latch.countDown());
            assertInstanceOf(NatsDispatcherWithExecutor.class, d);
            String subject = NUID.nextGlobalSequence();
            d.subscribe(subject);
            nc.publish(subject, null);
            assertTrue(latch.await(1, TimeUnit.SECONDS));
        }
    }
}
