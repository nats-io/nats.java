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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.Subscription;



// Some tests are a bit tricky, and depend on the fact that the dispatcher
// uses a single queue, so the "subject" messages go through before
// the done message (or should) - wanted to note that somewhere

public class DispatcherTests {
    @Test
    public void testSingleMessage() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testMultiSubject() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).maxReconnects(0).build())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testMultiMessage() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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

    @Test(expected=TimeoutException.class)
    public void testClose() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    }


    @Test
    public void testQueueSubscribers() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                 Connection nc = Nats.connect(ts.getURI())) {
            int msgs = 100;
            AtomicInteger received = new AtomicInteger();
            AtomicInteger sub1Count = new AtomicInteger();
            AtomicInteger sub2Count = new AtomicInteger();

            final CompletableFuture<Boolean> done1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> done2 = new CompletableFuture<>();

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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

    @Test(expected = IllegalStateException.class)
    public void testCantUnsubSubFromDispatcher()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
                try (NatsTestServer ts = new NatsTestServer(false);
                            Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    }

    @Test(expected = IllegalStateException.class)
    public void testCantAutoUnsubSubFromDispatcher()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
                try (NatsTestServer ts = new NatsTestServer(false);
                            Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    }

    @Test
    public void testPublishAndFlushFromCallback()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            final CompletableFuture<Message> msgFuture = new CompletableFuture<>();
            Dispatcher d = nc.createDispatcher((msg) -> {
                try {
                    nc.flush(Duration.ofMillis(1000));
                } catch (Exception ex) {
                    System.out.println("!!! Exception in callback");
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
    public void testUnsub() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 10;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testAutoUnsub() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> phase1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> phase2 = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testUnsubFromCallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testCloseFromCallback() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
            nc.flush(Duration.ofMillis(5000));// Get them all to the server

            nc.publish("done", new byte[16]); // when we get this we know the others are dispatched
            nc.flush(Duration.ofMillis(5000)); // Wait for the publish

            done.get(5000, TimeUnit.MILLISECONDS); // make sure we got them
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testDispatchHandlesExceptionInHandler() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe(null);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});

            d.subscribe("");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptyQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("subject", "");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe(null, "quque");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("", "quque");
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void throwsOnCreateIfClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.close();
            nc.createDispatcher((msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void throwsOnSubscribeIfClosed() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            nc.close();
            d.subscribe("subject");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnSubscribeWhenClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            nc.closeDispatcher(d);
            d.subscribe("foo");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnUnsubscribeWhenClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("foo");
            nc.closeDispatcher(d);
            d.unsubscribe("foo");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnDoubleClose() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            nc.closeDispatcher(d);
            nc.closeDispatcher(d);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testThrowOnConnClosed() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            nc.close();
            nc.closeDispatcher(d);
            assertFalse(true);
        }
    }

    @Test
    public void testDoubleSubscribe() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testDoubleSubscribeWithCustomHandler() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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
    public void testDoubleSubscribeWithUnsubscribeAfterWithCustomHandler() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            final CompletableFuture<Boolean> done1 = new CompletableFuture<>();
            final CompletableFuture<Boolean> done2 = new CompletableFuture<>();
            int msgCount = 100;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

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

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubjectWithMessageHandler() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptyQueueWithMessageHandler() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("subject", "", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubjectWithQueueWithMessageHandler() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe(null, "quque", (msg) -> {});
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubjectWithQueueWithMessageHandler() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            Dispatcher d = nc.createDispatcher((msg) -> {});
            d.subscribe("", "quque", (msg) -> {});
            assertFalse(true);
        }
    }
}
