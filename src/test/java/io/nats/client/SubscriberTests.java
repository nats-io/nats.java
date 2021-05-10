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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.nats.client.utils.TestBase.standardCloseConnection;
import static io.nats.client.utils.TestBase.standardConnectionWait;
import static org.junit.jupiter.api.Assertions.*;

public class SubscriberTests {

    @Test
    public void testCreateInbox() throws IOException, InterruptedException {
        HashSet<String> check = new HashSet<>();
        try (NatsTestServer ts = new NatsTestServer(false);
            Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

            for (int i=0; i < 10_000; i++) {
                String inbox = nc.createInbox();
                assertFalse(check.contains(inbox));
                check.add(inbox);
            }
        }
    }

    @Test
    public void testSingleMessage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

            Subscription sub = nc.subscribe("subject");
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertTrue(sub.isActive());
            assertEquals("subject", msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
        }
    }

    @Test
    public void testMessageFromSubscriptionContainsConnection() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

            Subscription sub = nc.subscribe("subject");
            nc.publish("subject", new byte[16]);

            Message msg = sub.nextMessage(Duration.ofMillis(500));

            assertTrue(sub.isActive());
            assertEquals("subject", msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(16, msg.getData().length);
            assertSame(msg.getConnection(), nc);
        }
    }

    @Test
    public void testTabInProtocolLine() throws Exception {
        CompletableFuture<Boolean> gotSub = new CompletableFuture<>();
        CompletableFuture<Boolean> sendMsg = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String subLine = "";
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for SUB ...");
            try {
                subLine = r.readLine();
            } catch(Exception e) {
                gotSub.cancel(true);
                return;
            }

            if (subLine.startsWith("SUB")) {
                gotSub.complete(Boolean.TRUE);
            }

            String[] parts = subLine.split("\\s");
            String subject = parts[1];
            int subId = Integer.parseInt(parts[2]);

            try {
                sendMsg.get();
            } catch (Exception e) {
                //keep going
            }

            w.write("MSG\t"+subject+"\t"+subId+"\t0\r\n\r\n");
            w.flush();
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer);
                    Connection  nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

            Subscription sub = nc.subscribe("subject");

            gotSub.get();
            sendMsg.complete(Boolean.TRUE);

            Message msg = sub.nextMessage(Duration.ZERO);//Duration.ofMillis(1000));

            assertTrue(sub.isActive());
            assertNotNull(msg);
            assertEquals("subject", msg.getSubject());
            assertEquals(sub, msg.getSubscription());
            assertNull(msg.getReplyTo());
            assertEquals(0, msg.getData().length);

            standardCloseConnection(nc);
        }
    }

    @Test
    public void testMultiMessage() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

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
        }
    }

    @Test
    public void testQueueSubscribers() throws IOException, InterruptedException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);

            int msgs = 100;
            int received = 0;
            int sub1Count = 0;
            int sub2Count = 0;
            Message msg;

            Subscription sub1 = nc.subscribe("subject", "queue");
            Subscription sub2 = nc.subscribe("subject", "queue");

            for (int i = 0; i < msgs; i++) {
                nc.publish("subject", new byte[16]);
            }

            nc.flush(Duration.ofMillis(200));// Get them all to the server

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
        }
    }

    @Test
    public void testUnsubscribe() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject");
                nc.publish("subject", new byte[16]);

                Message msg = sub.nextMessage(Duration.ofMillis(500));
                assertNotNull(msg);

                sub.unsubscribe();
                assertFalse(sub.isActive());
                sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            }
        });
    }

    @Test
    public void testAutoUnsubscribe() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject").unsubscribe(1);
                nc.publish("subject", new byte[16]);

                Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
                assertNotNull(msg);

                sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            }
        });
    }

    @Test
    public void testMultiAutoUnsubscribe() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                int msgCount = 10;
                Subscription sub = nc.subscribe("subject").unsubscribe(msgCount);

                for (int i = 0; i < msgCount; i++) {
                    nc.publish("subject", new byte[16]);
                }

                Message msg;
                for (int i = 0; i < msgCount; i++) {
                    msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
                    assertNotNull(msg);
                }

                sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            }
        });
    }

    @Test
    public void testOnlyOneUnsubscribe() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject");

                sub.unsubscribe();
                sub.unsubscribe(); // Will throw an exception
            }
        });
    }

    @Test
    public void testOnlyOneAutoUnsubscribe() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject").unsubscribe(1);
                nc.publish("subject", new byte[16]);

                Message msg = sub.nextMessage(Duration.ofMillis(500)); // should get 1
                assertNotNull(msg);

                sub.unsubscribe(); // Will throw an exception
            }
        });
    }

    @Test
    public void testUnsubscribeInAnotherThread() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject");

                new Thread(sub::unsubscribe).start();

                sub.nextMessage(Duration.ofMillis(5000)); // throw
            }
        });
    }

    @Test
    public void testAutoUnsubAfterMaxIsReached() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject");

                int msgCount = 10;
                for (int i = 0; i < msgCount; i++) {
                    nc.publish("subject", new byte[16]);
                }

                nc.flush(Duration.ofMillis(1000)); // Slow things down so we have time to unsub

                for (int i = 0; i < msgCount; i++) {
                    sub.nextMessage(null);
                }

                sub.unsubscribe(msgCount); // we already have that many

                sub.nextMessage(Duration.ofMillis(500)); // Will throw an exception
            }
        });
    }

    @Test
    public void testThrowOnNullSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe(null);
            }
        });
    }

    @Test
    public void testThrowOnEmptySubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe("");
            }
        });
    }

    @Test
    public void testThrowOnNullQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe("subject", null);
            }
        });
    }

    @Test
    public void testThrowOnEmptyQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe("subject", "");
            }
        });
    }

    @Test
    public void testThrowOnNullSubjectWithQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe(null, "quque");
            }
        });
    }

    @Test
    public void testThrowOnEmptySubjectWithQueue() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.subscribe("", "quque");
            }
        });
    }

    @Test
    public void throwsOnSubscribeIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.subscribe("subject");
            }
        });
    }

    @Test
    public void throwsOnUnsubscribeIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                Subscription sub = nc.subscribe("subject");
                nc.close();
                sub.unsubscribe();
            }
        });
    }

    @Test
    public void throwsOnAutoUnsubscribeIfClosed() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);
                Subscription sub = nc.subscribe("subject");
                nc.close();
                sub.unsubscribe(1);
            }
        });
    }

    @Test
    public void testUnsubscribeWhileWaiting() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                standardConnectionWait(nc);

                Subscription sub = nc.subscribe("subject");
                nc.flush(Duration.ofMillis(1000));

                new Thread(()->{
                    try { Thread.sleep(100); } catch(Exception e) { /* ignored */ }
                    sub.unsubscribe();
                }).start();

                sub.nextMessage(Duration.ofMillis(5000)); // Should throw
            }
        });
    }

    @Test
    public void testWhiteSpaceInSubject() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
            Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("foo bar"));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("foo\tbar"));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("foo "));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(" foo"));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe(" foo"));
        }
    }

    @Test
    public void testWhiteSpaceInQueue() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
            Connection nc = Nats.connect(ts.getURI())) {
            standardConnectionWait(nc);
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("test", "foo bar"));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("test", "foo\tbar"));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("test", "foo "));
            assertThrows(IllegalArgumentException.class, () -> nc.subscribe("test", " foo"));
        }
    }
}