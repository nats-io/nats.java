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

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

public class SubscriberTests {
    @Test
    public void testSingleMessage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertEquals("subject", msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testMultiMessage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");
            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertEquals("subject", msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNotNull(msg);
            msg = sub.nextMessage(Duration.ofMillis(100));
            assertNull(msg);

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testQueueSubscribers() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            int msgs = 100;
            int received = 0;
            int sub1Count = 0;
            int sub2Count = 0;
            Message msg;
            Connection nc = Nats.connect(ts.getURI());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub1 = nc.subscribe("subject", "queue");
            Subscription sub2 = nc.subscribe("subject", "queue");

            for (int i = 0; i < msgs; i++) {
                nc.publish("subject", new byte[16]);
            }

            nc.flush(Duration.ZERO);// Get them all to the server

            for (int i = 0; i < msgs; i++) {
                msg = sub1.nextMessage(null);

                if (msg != null) {
                    assertEquals("subject", msg.getSubject());
                    assertNull(msg.getReplyTo());
                    assertEquals(16, msg.getData().length);
                    received++;
                    sub1Count++;
                }
            }

            for (int i = 0; i < msgs; i++) {
                msg = sub2.nextMessage(null);

                if (msg != null) {
                    assertEquals("subject", msg.getSubject());
                    assertNull(msg.getReplyTo());
                    assertEquals(16, msg.getData().length);
                    received++;
                    sub2Count++;
                }
            }

            assertEquals(msgs, received);
            assertEquals(msgs, sub1Count + sub2Count);

            System.out.println("### Sub 1 " + sub1Count);
            System.out.println("### Sub 2 " + sub2Count);

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribe() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));
            assertNotNull(msg);

            sub.unsubscribe();
            msg = sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testAutoUnsubscribe() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject").unsubscribe(1);
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
            assertNotNull(msg);

            msg = sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiAutoUnsubscribe() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            int msgCount = 10;
            Message msg = null;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject").unsubscribe(msgCount);

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }

            for (int i = 0; i < msgCount; i++) {
                msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
                assertNotNull(msg);
            }

            msg = sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOnlyOneUnsubscribe() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");

            sub.unsubscribe();
            sub.unsubscribe(); // Will throw an exception

            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testOnlyOneAutoUnsubscribe() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject").unsubscribe(1);
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
            assertNotNull(msg);

            sub.unsubscribe(); // Will throw an exception
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUnsubscribeInAnotherThread() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");

            Thread t = new Thread(() -> {
                sub.unsubscribe();
            });
            t.start();

            sub.nextMessage(Duration.ofMillis(5000)); // throw
            assertFalse(true);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testAutoUnsubAfterMaxIsReached() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            int msgCount = 10;
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            Subscription sub = nc.subscribe("subject");

            for (int i = 0; i < msgCount; i++) {
                nc.publish("subject", new byte[16]);
            }

            nc.flush(Duration.ofMillis(1000)); // Slow things down so we have time to unsub

            for (int i = 0; i < msgCount; i++) {
                sub.nextMessage(null);
            }

            sub.unsubscribe(msgCount); // we already have that many

            sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe(null);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubject() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe("");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe("subject", null);
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptyQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe("subject", "");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnNullSubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe(null, "quque");
            assertFalse(true);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testThrowOnEmptySubjectWithQueue() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.subscribe("", "quque");
            assertFalse(true);
        }
    }
}